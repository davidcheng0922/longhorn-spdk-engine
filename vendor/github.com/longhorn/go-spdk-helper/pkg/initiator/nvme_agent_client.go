package initiator

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

const (
	NvmeCliAgentAddressEnv = "NVME_CLI_AGENT_ADDRESS"

	longhornControlPathEnv              = "LONGHORN_CONTROL_PATH"
	defaultLonghornControlPath          = "/var/lib/longhorn"
	longhornHostPrefix                  = "/host"
	unixDomainSocketDirectorySubpath    = "unix-domain-socket"
	defaultNvmeCliAgentSocketName       = "nvme-agent.sock"
	defaultNvmeCliAgentAddressURIScheme = "unix://"

	nvmeCliAgentRequestTimeout = 3 * time.Minute
)

type NvmeCliClient interface {
	DiscoverTarget(initiatorName, ip, port string) (string, error)
	ConnectTarget(initiatorName, ip, port, nqn string, skipDiscovery bool) (string, error)
	DisconnectTarget(initiatorName, nqn string) error
	DisconnectController(initiatorName, nqn, ip, port string) error
	GetDevices(initiatorName, ip, port, nqn string) ([]Device, error)
	GetSubsystems(initiatorName string) ([]Subsystem, error)
	Close() error
}

var (
	nvmeCliClientMu sync.Mutex
	nvmeCliClient   NvmeCliClient
)

func SetNvmeCliClient(client NvmeCliClient) {
	nvmeCliClientMu.Lock()
	defer nvmeCliClientMu.Unlock()

	nvmeCliClient = client
}

func SetNvmeCliAgentAddress(address string) error {
	client, err := NewGRPCNvmeCliClient(address)
	if err != nil {
		return err
	}
	SetNvmeCliClient(client)
	return nil
}

func getNvmeCliClient() (NvmeCliClient, error) {
	nvmeCliClientMu.Lock()
	defer nvmeCliClientMu.Unlock()

	if nvmeCliClient != nil {
		return nvmeCliClient, nil
	}

	address := os.Getenv(NvmeCliAgentAddressEnv)
	if address == "" {
		address = GetDefaultNvmeCliAgentAddress()
	}

	client, err := NewGRPCNvmeCliClient(address)
	if err != nil {
		return nil, err
	}
	nvmeCliClient = client
	return nvmeCliClient, nil
}

type GRPCNvmeCliClient struct {
	address string
	conn    *grpc.ClientConn
	client  spdkrpc.NvmeAgentServiceClient
}

func NewGRPCNvmeCliClient(address string) (*GRPCNvmeCliClient, error) {
	if address == "" {
		address = GetDefaultNvmeCliAgentAddress()
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithNoProxy(),
		grpc.WithDisableServiceConfig(),
	}
	if strings.HasPrefix(address, "unix://") {
		opts = append(opts, grpc.WithContextDialer(unixNvmeCliAgentDialer))
	}

	conn, err := grpc.NewClient(
		address,
		opts...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create NVMe CLI agent client for %s", address)
	}

	return &GRPCNvmeCliClient{
		address: address,
		conn:    conn,
		client:  spdkrpc.NewNvmeAgentServiceClient(conn),
	}, nil
}

func GetDefaultNvmeCliAgentAddress() string {
	return defaultNvmeCliAgentAddressURIScheme + GetDefaultNvmeCliAgentSocketPath()
}

func GetDefaultNvmeCliAgentSocketPath() string {
	controlPath := strings.TrimSpace(os.Getenv(longhornControlPathEnv))
	if controlPath == "" {
		controlPath = defaultLonghornControlPath
	}

	controlPath = filepath.Clean(controlPath)
	if !filepath.IsAbs(controlPath) || controlPath == string(filepath.Separator) || controlPath == "." {
		controlPath = defaultLonghornControlPath
	}

	return filepath.Join(longhornHostPrefix, strings.TrimLeft(controlPath, string(filepath.Separator)), unixDomainSocketDirectorySubpath, defaultNvmeCliAgentSocketName)
}

func unixNvmeCliAgentDialer(ctx context.Context, address string) (net.Conn, error) {
	socketPath := strings.TrimPrefix(address, "unix://")
	return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
}

func (c *GRPCNvmeCliClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *GRPCNvmeCliClient) DiscoverTarget(initiatorName, ip, port string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCliAgentRequestTimeout)
	defer cancel()

	resp, err := c.client.DiscoverTarget(ctx, &spdkrpc.NvmeDiscoverTargetRequest{
		InitiatorName: initiatorName,
		Target:        newProtoNvmeTarget(ip, port, ""),
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to discover NVMe target %s:%s via agent %s", ip, port, c.address)
	}
	return resp.GetSubsystemNqn(), nil
}

func (c *GRPCNvmeCliClient) ConnectTarget(initiatorName, ip, port, nqn string, skipDiscovery bool) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCliAgentRequestTimeout)
	defer cancel()

	resp, err := c.client.ConnectTarget(ctx, &spdkrpc.NvmeConnectTargetRequest{
		InitiatorName: initiatorName,
		Target:        newProtoNvmeTarget(ip, port, nqn),
		SkipDiscovery: skipDiscovery,
	})
	if err != nil {
		return "", errors.Wrapf(err, "failed to connect NVMe target %s:%s nqn=%s via agent %s", ip, port, nqn, c.address)
	}
	return resp.GetControllerName(), nil
}

func (c *GRPCNvmeCliClient) DisconnectTarget(initiatorName, nqn string) error {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCliAgentRequestTimeout)
	defer cancel()

	_, err := c.client.DisconnectTarget(ctx, &spdkrpc.NvmeDisconnectTargetRequest{
		InitiatorName: initiatorName,
		SubsystemNqn:  nqn,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to disconnect NVMe target nqn=%s via agent %s", nqn, c.address)
	}
	return nil
}

func (c *GRPCNvmeCliClient) DisconnectController(initiatorName, nqn, ip, port string) error {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCliAgentRequestTimeout)
	defer cancel()

	_, err := c.client.DisconnectController(ctx, &spdkrpc.NvmeDisconnectControllerRequest{
		InitiatorName: initiatorName,
		Target:        newProtoNvmeTarget(ip, port, nqn),
	})
	if err != nil {
		return errors.Wrapf(err, "failed to disconnect NVMe controller %s:%s nqn=%s via agent %s", ip, port, nqn, c.address)
	}
	return nil
}

func (c *GRPCNvmeCliClient) GetDevices(initiatorName, ip, port, nqn string) ([]Device, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCliAgentRequestTimeout)
	defer cancel()

	resp, err := c.client.GetDevices(ctx, &spdkrpc.NvmeGetDevicesRequest{
		InitiatorName: initiatorName,
		Target:        newProtoNvmeTarget(ip, port, nqn),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get NVMe devices %s:%s nqn=%s via agent %s", ip, port, nqn, c.address)
	}
	return nvmeDevicesFromProto(resp.GetDevices()), nil
}

func (c *GRPCNvmeCliClient) GetSubsystems(initiatorName string) ([]Subsystem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), nvmeCliAgentRequestTimeout)
	defer cancel()

	resp, err := c.client.GetSubsystems(ctx, &spdkrpc.NvmeGetSubsystemsRequest{
		InitiatorName: initiatorName,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get NVMe subsystems via agent %s", c.address)
	}
	return nvmeSubsystemsFromProto(resp.GetSubsystems()), nil
}

func newProtoNvmeTarget(ip, port, nqn string) *spdkrpc.NvmeTarget {
	return &spdkrpc.NvmeTarget{
		TransportType:      DefaultTransportType,
		TransportAddress:   ip,
		TransportServiceId: port,
		SubsystemNqn:       nqn,
	}
}

func nvmeDevicesFromProto(devices []*spdkrpc.NvmeDevice) []Device {
	res := make([]Device, 0, len(devices))
	for _, device := range devices {
		if device == nil {
			continue
		}
		res = append(res, Device{
			Subsystem:    device.GetSubsystem(),
			SubsystemNQN: device.GetSubsystemNqn(),
			Controllers:  nvmeControllersFromProto(device.GetControllers()),
			Namespaces:   nvmeNamespacesFromProto(device.GetNamespaces()),
		})
	}
	return res
}

func nvmeControllersFromProto(controllers []*spdkrpc.NvmeController) []Controller {
	res := make([]Controller, 0, len(controllers))
	for _, controller := range controllers {
		if controller == nil {
			continue
		}
		res = append(res, Controller{
			Controller: controller.GetController(),
			Transport:  controller.GetTransport(),
			Address:    controller.GetAddress(),
			State:      controller.GetState(),
		})
	}
	return res
}

func nvmeNamespacesFromProto(namespaces []*spdkrpc.NvmeNamespace) []Namespace {
	res := make([]Namespace, 0, len(namespaces))
	for _, namespace := range namespaces {
		if namespace == nil {
			continue
		}
		res = append(res, Namespace{
			NameSpace:    namespace.GetNamespaceName(),
			NSID:         namespace.GetNsid(),
			UsedBytes:    namespace.GetUsedBytes(),
			MaximumLBA:   namespace.GetMaximumLba(),
			PhysicalSize: namespace.GetPhysicalSize(),
			SectorSize:   namespace.GetSectorSize(),
		})
	}
	return res
}

func nvmeSubsystemsFromProto(subsystems []*spdkrpc.NvmeSubsystem) []Subsystem {
	res := make([]Subsystem, 0, len(subsystems))
	for _, subsystem := range subsystems {
		if subsystem == nil {
			continue
		}
		res = append(res, Subsystem{
			Name:  subsystem.GetName(),
			NQN:   subsystem.GetNqn(),
			Paths: nvmePathsFromProto(subsystem.GetPaths()),
		})
	}
	return res
}

func nvmePathsFromProto(paths []*spdkrpc.NvmePath) []Path {
	res := make([]Path, 0, len(paths))
	for _, path := range paths {
		if path == nil {
			continue
		}
		res = append(res, Path{
			Name:      path.GetName(),
			Transport: path.GetTransport(),
			Address:   path.GetAddress(),
			State:     path.GetState(),
		})
	}
	return res
}
