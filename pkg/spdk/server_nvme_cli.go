package spdk

import (
	"context"
	"sync"

	commonns "github.com/longhorn/go-common-libs/ns"
	commontypes "github.com/longhorn/go-common-libs/types"
	helperinitiator "github.com/longhorn/go-spdk-helper/pkg/initiator"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NvmeCliServer struct {
	spdkrpc.UnimplementedNvmeAgentServiceServer

	executor *commonns.Executor
	mu       sync.Mutex
}

func NewNvmeCliServer() (*NvmeCliServer, error) {
	executor, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create NVMe CLI executor")
	}

	return &NvmeCliServer{
		executor: executor,
	}, nil
}

func (s *NvmeCliServer) DiscoverTarget(_ context.Context, req *spdkrpc.NvmeDiscoverTargetRequest) (*spdkrpc.NvmeDiscoverTargetResponse, error) {
	if req.GetInitiatorName() == "" {
		return nil, status.Error(codes.InvalidArgument, "initiator_name is required")
	}
	if err := validateNvmeTarget(req.GetTarget(), false); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	target := req.GetTarget()
	nqn, err := helperinitiator.DiscoverTarget(target.GetTransportAddress(), target.GetTransportServiceId(), s.executor)
	if err != nil {
		return nil, toNvmeCliGRPCError(err, "failed to discover NVMe target")
	}

	return &spdkrpc.NvmeDiscoverTargetResponse{
		SubsystemNqn: nqn,
	}, nil
}

func (s *NvmeCliServer) ConnectTarget(_ context.Context, req *spdkrpc.NvmeConnectTargetRequest) (*spdkrpc.NvmeConnectTargetResponse, error) {
	if req.GetInitiatorName() == "" {
		return nil, status.Error(codes.InvalidArgument, "initiator_name is required")
	}
	if err := validateNvmeTarget(req.GetTarget(), req.GetSkipDiscovery()); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	target := req.GetTarget()
	nqn := target.GetSubsystemNqn()
	if !req.GetSkipDiscovery() && nqn == "" {
		discoveredNQN, err := helperinitiator.DiscoverTarget(target.GetTransportAddress(), target.GetTransportServiceId(), s.executor)
		if err != nil {
			return nil, toNvmeCliGRPCError(err, "failed to discover NVMe target before connect")
		}
		nqn = discoveredNQN
	}

	controllerName, err := helperinitiator.ConnectTarget(target.GetTransportAddress(), target.GetTransportServiceId(), nqn, s.executor)
	if err != nil {
		return nil, toNvmeCliGRPCError(err, "failed to connect NVMe target")
	}

	devices, err := helperinitiator.GetDevices(target.GetTransportAddress(), target.GetTransportServiceId(), nqn, s.executor)
	if err != nil {
		return nil, toNvmeCliGRPCError(err, "failed to get NVMe devices after connect")
	}

	subsystems, err := helperinitiator.GetSubsystems(s.executor)
	if err != nil {
		return nil, toNvmeCliGRPCError(err, "failed to get NVMe subsystems after connect")
	}

	return &spdkrpc.NvmeConnectTargetResponse{
		SubsystemNqn:   nqn,
		ControllerName: controllerName,
		Devices:        toProtoNvmeDevices(devices),
		Subsystems:     toProtoNvmeSubsystems(subsystems),
	}, nil
}

func (s *NvmeCliServer) DisconnectTarget(_ context.Context, req *spdkrpc.NvmeDisconnectTargetRequest) (*spdkrpc.NvmeDisconnectTargetResponse, error) {
	if req.GetInitiatorName() == "" {
		return nil, status.Error(codes.InvalidArgument, "initiator_name is required")
	}
	if req.GetSubsystemNqn() == "" {
		return nil, status.Error(codes.InvalidArgument, "subsystem_nqn is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := helperinitiator.DisconnectTarget(req.GetSubsystemNqn(), s.executor); err != nil {
		return nil, toNvmeCliGRPCError(err, "failed to disconnect NVMe target")
	}

	return &spdkrpc.NvmeDisconnectTargetResponse{}, nil
}

func (s *NvmeCliServer) DisconnectController(_ context.Context, req *spdkrpc.NvmeDisconnectControllerRequest) (*spdkrpc.NvmeDisconnectControllerResponse, error) {
	if req.GetInitiatorName() == "" {
		return nil, status.Error(codes.InvalidArgument, "initiator_name is required")
	}
	if err := validateNvmeTarget(req.GetTarget(), true); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	target := req.GetTarget()
	if err := helperinitiator.DisconnectController(target.GetSubsystemNqn(), target.GetTransportAddress(), target.GetTransportServiceId(), s.executor); err != nil {
		return nil, toNvmeCliGRPCError(err, "failed to disconnect NVMe controller")
	}

	return &spdkrpc.NvmeDisconnectControllerResponse{}, nil
}

func (s *NvmeCliServer) GetDevices(_ context.Context, req *spdkrpc.NvmeGetDevicesRequest) (*spdkrpc.NvmeGetDevicesResponse, error) {
	if req.GetInitiatorName() == "" {
		return nil, status.Error(codes.InvalidArgument, "initiator_name is required")
	}
	if err := validateNvmeTarget(req.GetTarget(), true); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	target := req.GetTarget()
	devices, err := helperinitiator.GetDevices(target.GetTransportAddress(), target.GetTransportServiceId(), target.GetSubsystemNqn(), s.executor)
	if err != nil {
		return nil, toNvmeCliGRPCError(err, "failed to get NVMe devices")
	}

	return &spdkrpc.NvmeGetDevicesResponse{
		Devices: toProtoNvmeDevices(devices),
	}, nil
}

func (s *NvmeCliServer) GetSubsystems(_ context.Context, req *spdkrpc.NvmeGetSubsystemsRequest) (*spdkrpc.NvmeGetSubsystemsResponse, error) {
	if req.GetInitiatorName() == "" {
		return nil, status.Error(codes.InvalidArgument, "initiator_name is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	subsystems, err := helperinitiator.GetSubsystems(s.executor)
	if err != nil {
		return nil, toNvmeCliGRPCError(err, "failed to get NVMe subsystems")
	}

	return &spdkrpc.NvmeGetSubsystemsResponse{
		Subsystems: toProtoNvmeSubsystems(subsystems),
	}, nil
}

func validateNvmeTarget(target *spdkrpc.NvmeTarget, requireNQN bool) error {
	if target == nil {
		return status.Error(codes.InvalidArgument, "target is required")
	}
	if target.GetTransportAddress() == "" {
		return status.Error(codes.InvalidArgument, "target.transport_address is required")
	}
	if target.GetTransportServiceId() == "" {
		return status.Error(codes.InvalidArgument, "target.transport_service_id is required")
	}
	if requireNQN && target.GetSubsystemNqn() == "" {
		return status.Error(codes.InvalidArgument, "target.subsystem_nqn is required")
	}
	if transportType := target.GetTransportType(); transportType != "" && transportType != helperinitiator.DefaultTransportType {
		return status.Errorf(codes.InvalidArgument, "unsupported target.transport_type %q", transportType)
	}
	return nil
}

func toNvmeCliGRPCError(err error, msg string) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Internal, "%s: %v", msg, err)
}

func toProtoNvmeDevices(devices []helperinitiator.Device) []*spdkrpc.NvmeDevice {
	res := make([]*spdkrpc.NvmeDevice, 0, len(devices))
	for _, device := range devices {
		res = append(res, &spdkrpc.NvmeDevice{
			Subsystem:    device.Subsystem,
			SubsystemNqn: device.SubsystemNQN,
			Controllers:  toProtoNvmeControllers(device.Controllers),
			Namespaces:   toProtoNvmeNamespaces(device.Namespaces),
		})
	}
	return res
}

func toProtoNvmeControllers(controllers []helperinitiator.Controller) []*spdkrpc.NvmeController {
	res := make([]*spdkrpc.NvmeController, 0, len(controllers))
	for _, controller := range controllers {
		res = append(res, &spdkrpc.NvmeController{
			Controller: controller.Controller,
			Transport:  controller.Transport,
			Address:    controller.Address,
			State:      controller.State,
		})
	}
	return res
}

func toProtoNvmeNamespaces(namespaces []helperinitiator.Namespace) []*spdkrpc.NvmeNamespace {
	res := make([]*spdkrpc.NvmeNamespace, 0, len(namespaces))
	for _, namespace := range namespaces {
		res = append(res, &spdkrpc.NvmeNamespace{
			NamespaceName: namespace.NameSpace,
			Nsid:          namespace.NSID,
			UsedBytes:     namespace.UsedBytes,
			MaximumLba:    namespace.MaximumLBA,
			PhysicalSize:  namespace.PhysicalSize,
			SectorSize:    namespace.SectorSize,
		})
	}
	return res
}

func toProtoNvmeSubsystems(subsystems []helperinitiator.Subsystem) []*spdkrpc.NvmeSubsystem {
	res := make([]*spdkrpc.NvmeSubsystem, 0, len(subsystems))
	for _, subsystem := range subsystems {
		res = append(res, &spdkrpc.NvmeSubsystem{
			Name:  subsystem.Name,
			Nqn:   subsystem.NQN,
			Paths: toProtoNvmePaths(subsystem.Paths),
		})
	}
	return res
}

func toProtoNvmePaths(paths []helperinitiator.Path) []*spdkrpc.NvmePath {
	res := make([]*spdkrpc.NvmePath, 0, len(paths))
	for _, path := range paths {
		res = append(res, &spdkrpc.NvmePath{
			Name:      path.Name,
			Transport: path.Transport,
			Address:   path.Address,
			State:     path.State,
		})
	}
	return res
}
