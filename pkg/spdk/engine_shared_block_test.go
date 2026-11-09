package spdk

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	. "gopkg.in/check.v1"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	lhtypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

func (s *TestSuite) TestGetTargetCntlidRange(c *C) {
	const ip = "10.42.1.21"

	// Without shared block the target keeps accepting a single host.
	for _, port := range []int32{1, 20006, 65535} {
		minCntlid, maxCntlid := getTargetCntlidRange(ip, port, false)
		c.Assert(minCntlid, Equals, getTargetCntlid(ip, port))
		c.Assert(maxCntlid, Equals, minCntlid)
	}

	for _, port := range []int32{1, 20006, 20011, 65535} {
		minCntlid, maxCntlid := getTargetCntlidRange(ip, port, true)
		c.Assert(minCntlid >= 1, Equals, true)
		c.Assert(maxCntlid <= maxValidCntlid, Equals, true)
		c.Assert(int(maxCntlid)-int(minCntlid)+1, Equals, sharedBlockMaxFrontends)

		// Re-exposing the same address during an expansion must keep the range.
		minAgain, maxAgain := getTargetCntlidRange(ip, port, true)
		c.Assert(minAgain, Equals, minCntlid)
		c.Assert(maxAgain, Equals, maxCntlid)
	}
}

func (s *TestSuite) TestEngineSharedBlockRaidUUIDAndPtplFile(c *C) {
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, false, "/metadata")
	c.Assert(e.getRaidBdevCreateUUID(), Equals, "")
	c.Assert(e.getReservationPtplFile(), Equals, "")

	e = NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, "/metadata")
	c.Assert(e.getRaidBdevCreateUUID(), Equals, getStableVolumeRaidUUID("vol-a"))
	c.Assert(e.getRaidBdevCreateUUID(), Not(Equals), getStableVolumeNsUUID("vol-a"))
	c.Assert(e.getReservationPtplFile(), Equals, filepath.Join("/metadata", reservationPtplSubDir, "vol-a", "0.json"))
	e.sharedBlockState.reservationEpoch = 3
	c.Assert(e.getReservationPtplFile(), Equals, filepath.Join("/metadata", reservationPtplSubDir, "vol-a", "3.json"))

	e = NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, "")
	c.Assert(e.getReservationPtplFile(), Equals, "")
}

func (s *TestSuite) TestEngineSharedBlockReportedInProto(c *C) {
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, false, "")
	c.Assert(e.getWithoutLock().SharedBlock, Equals, false)

	e = NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, "")
	c.Assert(e.getWithoutLock().SharedBlock, Equals, true)
}

func (s *TestSuite) TestEngineFrontendSharedBlockOnlySupportsBlockdev(c *C) {
	for _, frontend := range []string{lhtypes.FrontendSPDKTCPNvmf, lhtypes.FrontendUBLK} {
		ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", frontend, 1048576, 0, 0, make(chan interface{}, 1), nil, true)

		err := ef.handleFrontend(nil, "10.0.0.1:20006")
		c.Assert(err, ErrorMatches, ".*not supported for shared block engine frontend.*")
	}

	// The maintenance mode attaches a shared block volume without a frontend.
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	c.Assert(ef.handleFrontend(nil, "10.0.0.1:20006"), IsNil)
}

// The engine registers a shared block engine frontend before it connects, so
// suspending it while it is creating must fail, as it can connect afterwards.
func (s *TestSuite) TestEngineFrontendSharedBlockRejectsSuspendWhileCreating(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.State = lhtypes.InstanceStateRunning
	ef.isCreating = true

	err := ef.SuspendWithOptions(nil, false, false)
	c.Assert(err, ErrorMatches, ".*engine frontend ef-a is creating.*")
	c.Assert(string(ef.State), Equals, string(lhtypes.InstanceStateRunning))
}

func (s *TestSuite) TestEngineFrontendSharedBlockRejectsSnapshotCreateWithoutSuspend(c *C) {
	for _, tc := range []struct {
		state    lhtypes.InstanceState
		endpoint string
	}{
		{state: lhtypes.InstanceStateRunning, endpoint: ""},
		{state: lhtypes.InstanceStateError, endpoint: "/dev/longhorn/vol-a"},
	} {
		ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
		ef.State = tc.state
		ef.Endpoint = tc.endpoint

		_, err := ef.SnapshotCreate("snap-a")
		c.Assert(errors.Is(err, ErrEngineFrontendLifecyclePrecondition), Equals, true)
		c.Assert(string(ef.State), Equals, string(tc.state))
	}
}

func (s *TestSuite) TestEngineFrontendSharedBlockSnapshotCreateInMaintenanceMode(c *C) {
	errEngineCalled := errors.New("engine called")
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendEmpty, 1048576, 0, 0, make(chan interface{}, 1),
		func(string) (*client.SPDKClient, error) { return nil, errEngineCalled }, true)
	ef.State = lhtypes.InstanceStateRunning

	_, err := ef.SnapshotCreate("snap-a")
	c.Assert(errors.Is(err, errEngineCalled), Equals, true)
}

func (s *TestSuite) TestEngineFrontendSharedBlockRejectsSwitchover(c *C) {
	ef := NewEngineFrontend("ef-a", "engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.State = lhtypes.InstanceStateRunning

	err := ef.SwitchOverTarget(nil, "engine-b", "10.0.0.2:3000", "")
	c.Assert(errors.Is(err, ErrSwitchOverTargetInvalidInput), Equals, true)
	c.Assert(ef.EngineName, Equals, "engine-a")
	c.Assert(ef.getWithoutLock().SharedBlock, Equals, true)
}

func (s *TestSuite) TestSaveAndLoadEngineFrontendRecordPreservesSharedBlock(c *C) {
	tmpDir := c.MkDir()

	ef := NewEngineFrontend("ef-1", "engine-a", "vol-a",
		lhtypes.FrontendSPDKTCPBlockdev, 1048576, 0, 0, make(chan interface{}, 1), nil, true)
	ef.NvmeTcpFrontend.TargetIP = "10.0.0.1"
	ef.NvmeTcpFrontend.TargetPort = 3000

	err := saveEngineFrontendRecord(tmpDir, ef)
	c.Assert(err, IsNil)

	records, err := loadEngineFrontendRecords(tmpDir)
	c.Assert(err, IsNil)
	c.Assert(len(records), Equals, 1)
	c.Assert(records[0].SharedBlock, Equals, true)
}

const (
	testReservationHostA = "0f2a8bc4-1d9e-4b5c-8a7f-3e6d2c1b0a99"
	testReservationHostB = "11111111-2222-3333-4444-555555555555"
)

func newTestReservationState(epoch uint64) *spdkrpc.ReservationState {
	return &spdkrpc.ReservationState{
		Epoch:        epoch,
		Type:         3,
		HolderKey:    0xAAAA,
		HolderHostId: testReservationHostA,
		Registrants: []*spdkrpc.ReservationRegistrant{
			{Key: 0xAAAA, HostId: testReservationHostA},
			{Key: 0xFFFFFFFFFFFFFFFF, HostId: testReservationHostB},
		},
	}
}

func (s *TestSuite) TestParseReservationPtplData(c *C) {
	const bdevUUID = "5a6c7d8e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"

	tests := []struct {
		name  string
		data  string
		valid bool
	}{
		{"written by SPDK", `{"ptpl":true,"rtype":3,"crkey":43690,"bdev_uuid":"` + bdevUUID + `","holder_uuid":"` + testReservationHostA + `","registrants":[{"rkey":43690,"host_uuid":"` + testReservationHostA + `"}]}`, true},
		{"bdev UUID in upper case", `{"ptpl":true,"rtype":3,"crkey":43690,"bdev_uuid":"` + strings.ToUpper(bdevUUID) + `","holder_uuid":"` + testReservationHostA + `","registrants":[{"rkey":43690,"host_uuid":"` + testReservationHostA + `"}]}`, true},
		{"registered without reservation", `{"ptpl":true,"rtype":0,"crkey":0,"bdev_uuid":"` + bdevUUID + `","holder_uuid":"","registrants":[{"rkey":43690,"host_uuid":"` + testReservationHostA + `"}]}`, true},
		{"no registrants", `{"ptpl":true,"rtype":0,"crkey":0,"bdev_uuid":"","holder_uuid":"","registrants":[]}`, true},
		{"empty", ``, false},
		{"whitespace only", " \n", false},
		{"truncated", `{"ptpl":true,"rtype":3,"crkey":43690,"bdev_uuid":"` + bdevUUID, false},
		{"bdev UUID missing", `{"ptpl":true,"registrants":[]}`, false},
		{"registrants missing", `{"ptpl":true,"bdev_uuid":"` + bdevUUID + `"}`, false},
		{"host UUID missing", `{"ptpl":true,"bdev_uuid":"` + bdevUUID + `","registrants":[{"rkey":1}]}`, false},
		{"holder key not registered", `{"ptpl":true,"rtype":3,"crkey":1,"bdev_uuid":"` + bdevUUID + `","registrants":[{"rkey":2,"host_uuid":"` + testReservationHostA + `"}]}`, false},
		{"other bdev UUID", `{"ptpl":true,"rtype":0,"crkey":0,"bdev_uuid":"` + testReservationHostA + `","registrants":[{"rkey":2,"host_uuid":"` + testReservationHostA + `"}]}`, false},
	}

	for _, test := range tests {
		_, err := parseReservationPtplData([]byte(test.data), bdevUUID)
		c.Assert(err == nil, Equals, test.valid, Commentf("%s: %v", test.name, err))
	}

	regs := make([]string, sharedBlockMaxFrontends+1)
	for i := range regs {
		regs[i] = fmt.Sprintf(`{"rkey":%d,"host_uuid":"%s"}`, i+1, testReservationHostA)
	}
	data := `{"ptpl":true,"bdev_uuid":"` + bdevUUID + `","registrants":[` + strings.Join(regs, ",") + `]}`
	_, err := parseReservationPtplData([]byte(data), bdevUUID)
	c.Assert(err, ErrorMatches, ".*exceed the maximum.*")
}

func (s *TestSuite) TestReservationStatePtplDataRoundTrip(c *C) {
	const bdevUUID = "5a6c7d8e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"

	state := newTestReservationState(7)
	data, err := reservationStateToPtplData(state, bdevUUID)
	c.Assert(err, IsNil)

	record, err := parseReservationPtplData(data, bdevUUID)
	c.Assert(err, IsNil)
	got := reservationPtplRecordToState(record, 7)
	c.Assert(proto.Equal(got, state), Equals, true, Commentf("got %v", got))

	// A state without registrants has nothing to restore.
	data, err = reservationStateToPtplData(&spdkrpc.ReservationState{Epoch: 7}, bdevUUID)
	c.Assert(err, IsNil)
	c.Assert(data, IsNil)

	// A state without persistence has no registrants.
	record, err = parseReservationPtplData([]byte(`{"ptpl":false,"bdev_uuid":"","registrants":[{"rkey":1,"host_uuid":"`+testReservationHostA+`"}]}`), bdevUUID)
	c.Assert(err, IsNil)
	got = reservationPtplRecordToState(record, 7)
	c.Assert(got.Epoch, Equals, uint64(7))
	c.Assert(len(got.Registrants), Equals, 0)
}

func (s *TestSuite) TestEnginePrepareReservationPtplFile(c *C) {
	metadataDir := c.MkDir()
	newEngine := func(seed *spdkrpc.ReservationState) *Engine {
		e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, metadataDir)
		e.sharedBlockState.reservationEpoch = seed.Epoch
		e.sharedBlockState.reservationSeed = seed
		return e
	}
	bdevUUID := getStableVolumeRaidUUID("vol-a")
	readState := func(file string) *spdkrpc.ReservationState {
		data, err := os.ReadFile(file)
		c.Assert(err, IsNil)
		record, err := parseReservationPtplData(data, bdevUUID)
		c.Assert(err, IsNil)
		return reservationPtplRecordToState(record, 0)
	}

	// The first engine of an epoch rebuilds the file from the synced state.
	synced := newTestReservationState(1)
	e := newEngine(synced)
	ptplFile := e.getReservationPtplFile()
	c.Assert(os.MkdirAll(filepath.Dir(ptplFile), 0755), IsNil)
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	c.Assert(len(readState(ptplFile).Registrants), Equals, 2)

	// A newer local state of the same epoch wins over the synced state, e.g.,
	// SPDK persisted a preemption the manager has not synced yet.
	newer := newTestReservationState(1)
	newer.Registrants = newer.Registrants[:1]
	data, err := reservationStateToPtplData(newer, bdevUUID)
	c.Assert(err, IsNil)
	c.Assert(os.WriteFile(ptplFile, data, 0644), IsNil)
	e = newEngine(synced)
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	c.Assert(len(readState(ptplFile).Registrants), Equals, 1)

	// A partial or empty local file falls back to the synced state.
	for _, partial := range [][]byte{data[:20], nil} {
		c.Assert(os.WriteFile(ptplFile, partial, 0644), IsNil)
		e = newEngine(synced)
		c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
		c.Assert(len(readState(ptplFile).Registrants), Equals, 2)
	}

	// A later expose, e.g., by an expansion, does not restore the synced state
	// again: an empty file means the hosts persisted nothing since then.
	c.Assert(os.WriteFile(ptplFile, nil, 0644), IsNil)
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	_, err = os.Stat(ptplFile)
	c.Assert(os.IsNotExist(err), Equals, true)

	// A new epoch ignores and removes the file of the previous epoch, which
	// may be stale.
	c.Assert(os.WriteFile(ptplFile, data, 0644), IsNil)
	e = newEngine(newTestReservationState(2))
	newFile := e.getReservationPtplFile()
	c.Assert(e.prepareReservationPtplFile(newFile), IsNil)
	c.Assert(len(readState(newFile).Registrants), Equals, 2)
	_, err = os.Stat(ptplFile)
	c.Assert(os.IsNotExist(err), Equals, true)

	// Without a synced state, there is nothing to restore.
	e = newEngine(&spdkrpc.ReservationState{Epoch: 3})
	ptplFile = e.getReservationPtplFile()
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	entries, err := os.ReadDir(filepath.Dir(ptplFile))
	c.Assert(err, IsNil)
	c.Assert(len(entries), Equals, 0)

	// A synced state SPDK cannot restore is discarded instead of failing the
	// engine target.
	invalid := newTestReservationState(4)
	invalid.HolderKey = 0x1234
	e = newEngine(invalid)
	ptplFile = e.getReservationPtplFile()
	c.Assert(e.prepareReservationPtplFile(ptplFile), IsNil)
	_, err = os.Stat(ptplFile)
	c.Assert(os.IsNotExist(err), Equals, true)
}

func (s *TestSuite) TestEngineGetReservationState(c *C) {
	metadataDir := c.MkDir()
	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, true, metadataDir)
	e.sharedBlockState.reservationEpoch = 5
	ptplFile := e.getReservationPtplFile()
	c.Assert(os.MkdirAll(filepath.Dir(ptplFile), 0755), IsNil)

	// Not reported before the ptpl file is prepared, even if the engine is
	// running, e.g., in the maintenance mode without a target.
	e.State = lhtypes.InstanceStateRunning
	c.Assert(e.getWithoutLock().Reservation, IsNil)
	e.sharedBlockState.reservationPrepared = true

	// No file means no persisted state.
	state := e.getWithoutLock().Reservation
	c.Assert(state, NotNil)
	c.Assert(state.Epoch, Equals, uint64(5))
	c.Assert(len(state.Registrants), Equals, 0)

	data, err := reservationStateToPtplData(newTestReservationState(5), getStableVolumeRaidUUID("vol-a"))
	c.Assert(err, IsNil)
	c.Assert(os.WriteFile(ptplFile, data, 0644), IsNil)
	state = e.getWithoutLock().Reservation
	c.Assert(proto.Equal(state, newTestReservationState(5)), Equals, true, Commentf("got %v", state))

	// A partial file is unknown, e.g., SPDK is writing it.
	c.Assert(os.WriteFile(ptplFile, data[:20], 0644), IsNil)
	c.Assert(e.getWithoutLock().Reservation, IsNil)

	// A just emptied file may be in the middle of a write.
	c.Assert(os.WriteFile(ptplFile, nil, 0644), IsNil)
	c.Assert(e.getWithoutLock().Reservation, IsNil)

	// An empty file that is old enough means the host did not request
	// persistence.
	old := time.Now().Add(-2 * reservationPtplEmptyFileMinAge)
	c.Assert(os.Chtimes(ptplFile, old, old), IsNil)
	state = e.getWithoutLock().Reservation
	c.Assert(state, NotNil)
	c.Assert(len(state.Registrants), Equals, 0)

	// Engines without shared block report nothing.
	other := NewEngine("engine-b", "vol-b", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 1), 0, nil, false, metadataDir)
	other.State = lhtypes.InstanceStateRunning
	other.sharedBlockState.reservationPrepared = true
	c.Assert(other.getWithoutLock().Reservation, IsNil)
}

func (f *fakeEngineFrontendSuspendServer) counts() (suspends, resumes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.suspends, f.resumes
}

// unreachableAddress returns an address no one listens on.
func unreachableAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return address
}

// newFreezeTestEngine returns a shared block engine whose target reports the
// given connected hosts, one set per query.
func newFreezeTestEngine(t *testing.T, connectedHosts ...[]string) *Engine {
	t.Helper()

	e := NewEngine("engine-a", "vol-a", lhtypes.FrontendSPDKTCPBlockdev, 1048576, make(chan interface{}, 10), 0, client.NewSPDKClient, true, "")
	e.NvmeTcpTarget = &NvmeTcpTarget{Nqn: "nqn-a"}
	original := sharedBlockNvmfSubsystemGetControllers
	t.Cleanup(func() { sharedBlockNvmfSubsystemGetControllers = original })
	sharedBlockNvmfSubsystemGetControllers = func(_ *spdkclient.Client, nqn string) ([]spdktypes.NvmfSubsystemController, error) {
		if nqn != "nqn-a" {
			t.Errorf("unexpected NQN %s", nqn)
		}
		if len(connectedHosts) == 0 {
			t.Fatal("unexpected connected host query")
		}
		controllers := []spdktypes.NvmfSubsystemController{}
		for _, host := range connectedHosts[0] {
			controllers = append(controllers, spdktypes.NvmfSubsystemController{Hostnqn: host})
		}
		connectedHosts = connectedHosts[1:]
		return controllers, nil
	}
	return e
}

func registerFrontendForTest(t *testing.T, e *Engine, name, address, hostNQN string) {
	t.Helper()

	if err := e.RegisterFrontend(name, address, hostNQN); err != nil {
		t.Fatalf("register engine frontend %s: %v", name, err)
	}
}

func TestSharedBlockFrontendsSuspended(t *testing.T) {
	caller, callerAddress := startFakeEngineFrontendSuspendServer(t, nil)
	other, otherAddress := startFakeEngineFrontendSuspendServer(t, nil)

	// The engine frontend on host-c is gone and its host is not connected, so
	// it cannot write.
	e := newFreezeTestEngine(t, []string{"host-a", "host-b"}, []string{"host-a", "host-b"})
	registerFrontendForTest(t, e, "ef-a", callerAddress, "host-a")
	registerFrontendForTest(t, e, "ef-b", otherAddress, "host-b")
	registerFrontendForTest(t, e, "ef-c", unreachableAddress(t), "host-c")

	worked := false
	err := e.withSharedBlockFrontendsSuspended(nil, "ef-a", false, false, func() error {
		worked = true
		if suspends, resumes := other.counts(); suspends != 1 || resumes != 0 {
			t.Errorf("ef-b is not suspended during the work: suspends %d, resumes %d", suspends, resumes)
		}
		if err := e.RegisterFrontend("ef-d", otherAddress, "host-d"); !errors.Is(err, ErrSharedBlockFrontendsFreezing) {
			t.Errorf("expected registration to be rejected during the work, got %v", err)
		}
		if err := e.withSharedBlockFrontendsSuspended(nil, "", false, false, func() error { return nil }); !errors.Is(err, ErrSharedBlockFrontendsFreezing) {
			t.Errorf("expected concurrent suspension to be rejected, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !worked {
		t.Fatal("work did not run")
	}
	if suspends, resumes := caller.counts(); suspends != 0 || resumes != 0 {
		t.Errorf("the caller is suspended by itself, got suspends %d, resumes %d", suspends, resumes)
	}
	if suspends, resumes := other.counts(); suspends != 1 || resumes != 1 {
		t.Errorf("ef-b must be suspended and resumed once, got suspends %d, resumes %d", suspends, resumes)
	}
	other.mu.Lock()
	defer other.mu.Unlock()
	if other.noFlush || other.noLockFs {
		t.Errorf("ef-b must be suspended with the given options, got noFlush %v, noLockFs %v", other.noFlush, other.noLockFs)
	}
	registerFrontendForTest(t, e, "ef-d", otherAddress, "host-d")
}

func TestSharedBlockFrontendsSuspendedFailure(t *testing.T) {
	tests := []struct {
		name           string
		suspendErr     error
		goneHost       string
		connectedHosts [][]string
		expectedErr    string
	}{
		{
			name:           "gone engine frontend whose host is connected",
			goneHost:       "host-c",
			connectedHosts: [][]string{{"host-b", "host-c"}},
			expectedErr:    "failed to suspend engine frontend ef-c",
		},
		{
			name:           "engine frontend fails to suspend",
			suspendErr:     grpcstatus.Error(grpccodes.FailedPrecondition, "engine frontend is creating"),
			connectedHosts: [][]string{{}},
			expectedErr:    "failed to suspend engine frontend ef-b",
		},
		{
			name:           "host connects without a suspended engine frontend",
			connectedHosts: [][]string{{"host-b"}, {"host-b", "host-x"}},
			expectedErr:    "host host-x is connected to engine engine-a without a suspended engine frontend",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			other, otherAddress := startFakeEngineFrontendSuspendServer(t, tt.suspendErr)

			e := newFreezeTestEngine(t, tt.connectedHosts...)
			registerFrontendForTest(t, e, "ef-b", otherAddress, "host-b")
			if tt.goneHost != "" {
				registerFrontendForTest(t, e, "ef-c", unreachableAddress(t), tt.goneHost)
			}

			err := e.withSharedBlockFrontendsSuspended(nil, "", false, false, func() error {
				t.Error("work must not run")
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), tt.expectedErr) {
				t.Fatalf("expected error %q, got %v", tt.expectedErr, err)
			}
			suspends, resumes := other.counts()
			if tt.suspendErr == nil && (suspends != 1 || resumes != 1) {
				t.Errorf("ef-b must be resumed: suspends %d, resumes %d", suspends, resumes)
			}
			if tt.suspendErr != nil && resumes != 0 {
				t.Errorf("ef-b failing to suspend must not be resumed, got resumes %d", resumes)
			}
			// The failure does not leave the engine frontends being suspended.
			registerFrontendForTest(t, e, "ef-d", otherAddress, "host-d")
		})
	}
}

// The engine frontends on all hosts must be suspended before the replica add
// does anything, and nothing is done if they cannot be.
func TestServerEngineReplicaAddSharedBlockSuspendFailure(t *testing.T) {
	e := newFreezeTestEngine(t, []string{"host-b"})
	e.State = lhtypes.InstanceStateRunning
	registerFrontendForTest(t, e, "ef-b", unreachableAddress(t), "host-b")
	srv := &Server{engineMap: map[string]*Engine{e.Name: e}}

	_, err := srv.EngineReplicaAdd(context.Background(), &spdkrpc.EngineReplicaAddRequest{
		EngineName:     e.Name,
		ReplicaName:    "replica-c",
		ReplicaAddress: "10.0.0.3:20001",
	})
	if err == nil || !strings.Contains(err.Error(), "failed to suspend engine frontend ef-b") {
		t.Fatalf("expected suspension failure, got %v", err)
	}
	if len(e.backends) != 0 {
		t.Fatalf("replica add must not start, got backends %v", e.backends)
	}
}

func TestSharedBlockReplicaAddFrontendSuspendResumeWrapper(t *testing.T) {
	ef, efAddress := startFakeEngineFrontendSuspendServer(t, nil)
	getSPDKClient := func() *spdkclient.Client { return nil }

	t.Run("snapshot step runs in the suspension of the caller", func(t *testing.T) {
		e := newFreezeTestEngine(t)
		registerFrontendForTest(t, e, "ef-b", efAddress, "host-b")
		wrapper := e.buildSharedBlockReplicaAddFrontendSuspendResumeWrapper(getSPDKClient)

		worked := false
		if err := wrapper(func() error { worked = true; return nil }, true); err != nil || !worked {
			t.Fatalf("expected work to run, got worked %v, err %v", worked, err)
		}
		if suspends, _ := ef.counts(); suspends != 0 {
			t.Fatalf("snapshot step must not suspend, got suspends %d", suspends)
		}
	})

	t.Run("finish step suspends the engine frontends", func(t *testing.T) {
		e := newFreezeTestEngine(t, []string{"host-b"}, []string{"host-b"})
		registerFrontendForTest(t, e, "ef-b", efAddress, "host-b")
		wrapper := e.buildSharedBlockReplicaAddFrontendSuspendResumeWrapper(getSPDKClient)

		workErr := errors.New("finish failed")
		err := wrapper(func() error {
			if suspends, resumes := ef.counts(); suspends != 1 || resumes != 0 {
				t.Errorf("ef-b is not suspended during the work: suspends %d, resumes %d", suspends, resumes)
			}
			return workErr
		}, false)
		if !errors.Is(err, workErr) {
			t.Fatalf("expected the work error, got %v", err)
		}
		if suspends, resumes := ef.counts(); suspends != 1 || resumes != 1 {
			t.Fatalf("ef-b must be suspended and resumed once, got suspends %d, resumes %d", suspends, resumes)
		}
		ef.mu.Lock()
		defer ef.mu.Unlock()
		if !ef.noFlush || !ef.noLockFs {
			t.Fatalf("ef-b must be suspended with noflush and nolockfs, got %v, %v", ef.noFlush, ef.noLockFs)
		}
	})

	t.Run("finish step proceeds without the suspension", func(t *testing.T) {
		e := newFreezeTestEngine(t, []string{"host-b"})
		registerFrontendForTest(t, e, "ef-b", unreachableAddress(t), "host-b")
		wrapper := e.buildSharedBlockReplicaAddFrontendSuspendResumeWrapper(getSPDKClient)

		worked := false
		if err := wrapper(func() error { worked = true; return nil }, false); err != nil || !worked {
			t.Fatalf("expected work to run, got worked %v, err %v", worked, err)
		}
	})
}
