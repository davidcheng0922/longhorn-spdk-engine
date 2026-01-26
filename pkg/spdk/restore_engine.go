package spdk

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"

	btypes "github.com/longhorn/backupstore/types"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
)

type EngineRestore struct {
	sync.RWMutex

	spdkClient *spdkclient.Client
	engine     *Engine

	Progress  int
	Error     string
	BackupURL string
	State     btypes.ProgressState

	// The snapshot file that stores the restored data in the end.
	SnapshotName string

	TargetAddress string

	LastRestored           string
	CurrentRestoringBackup string

	stopChan chan struct{}
	stopOnce sync.Once

	log logrus.FieldLogger
}

var _ backupstore.DeltaRestoreOperations = (*EngineRestore)(nil)

func NewEngineRestore(spdkClient *spdkclient.Client, snapshotName string, backupURL string, backupName string, engine *Engine) *EngineRestore {
	log := logrus.WithFields(logrus.Fields{
		"targetAddress": engine.restore.TargetAddress,
		"snapshotName":  snapshotName,
		"backupURL":     backupURL,
		"backupName":    backupName,
	})

	return &EngineRestore{
		spdkClient:             spdkClient,
		engine:                 engine,
		BackupURL:              backupURL,
		CurrentRestoringBackup: backupName,
		SnapshotName:           snapshotName,
		TargetAddress:          engine.restore.TargetAddress,
		State:                  btypes.ProgressStateInProgress,
		Progress:               0,
		stopChan:               make(chan struct{}),
		log:                    log,
	}
}

func (r *EngineRestore) StartNewRestore(backupURL string, currentRestoringBackup string, snapshotName string, validLastRestoredBackup bool) {
	r.Lock()
	defer r.Unlock()

	r.SnapshotName = snapshotName

	r.Progress = 0
	r.Error = ""
	r.BackupURL = backupURL
	r.State = btypes.ProgressStateInProgress

	if !validLastRestoredBackup {
		r.LastRestored = ""
	}

	r.CurrentRestoringBackup = currentRestoringBackup
}

func (r *EngineRestore) DeepCopy() *EngineRestore {
	r.RLock()
	defer r.RUnlock()

	return &EngineRestore{
		BackupURL:              r.BackupURL,
		CurrentRestoringBackup: r.CurrentRestoringBackup,
		LastRestored:           r.LastRestored,
		SnapshotName:           r.SnapshotName,
		State:                  r.State,
		Error:                  r.Error,
		Progress:               r.Progress,
	}
}

func (r *EngineRestore) OpenVolumeDev(volDevName string) (*os.File, string, error) {
	if r.engine.initiator == nil || r.engine.initiator.Endpoint == "" {
		return nil, "", fmt.Errorf("restore initiator is not initialized")
	}

	r.log.Infof("Opening NVMe device %v", r.engine.initiator.Endpoint)
	fh, err := os.OpenFile(r.engine.initiator.Endpoint, os.O_RDWR, 0666)
	if err != nil {
		return nil, "", errors.Wrapf(err, "failed to open NVMe device %v", r.engine.initiator.Endpoint)
	}
	return fh, r.engine.initiator.Endpoint, nil
}

func (r *EngineRestore) CloseVolumeDev(volDev *os.File) error {
	r.log.Infof("Closing NVMe device %v", r.engine.initiator.Endpoint)

	return volDev.Close()
}

func (r *EngineRestore) UpdateRestoreStatus(snapshot string, progress int, err error) {
	r.Lock()
	defer r.Unlock()

	r.Progress = progress

	if err != nil {
		if strings.Contains(err.Error(), btypes.ErrorMsgRestoreCancelled) {
			r.State = btypes.ProgressStateCanceled
			r.Error = err.Error()
		} else {
			r.State = btypes.ProgressStateError
			if r.Error != "" {
				r.Error = fmt.Sprintf("%v: %v", err.Error(), r.Error)
			} else {
				r.Error = err.Error()
			}
		}
	}
}

func (r *EngineRestore) FinishRestore() {
	r.Lock()
	defer r.Unlock()

	if r.State != btypes.ProgressStateError && r.State != btypes.ProgressStateCanceled {
		r.State = btypes.ProgressStateComplete
		r.LastRestored = r.CurrentRestoringBackup
		r.CurrentRestoringBackup = ""
	}
}

func (r *EngineRestore) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopChan)

		r.Lock()
		defer r.Unlock()
		r.State = btypes.ProgressStateCanceled
		r.Error = btypes.ErrorMsgRestoreCancelled
		r.Progress = 0
	})
}

func (r *EngineRestore) GetStopChan() chan struct{} {
	return r.stopChan
}
