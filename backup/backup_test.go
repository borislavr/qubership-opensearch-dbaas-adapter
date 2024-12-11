package backup

import (
	"context"
	"github.com/Netcracker/dbaas-opensearch-adapter/common"
	"github.com/stretchr/testify/assert"
	"net/http"
	"os"
	"testing"
)

var backupProvider BackupProvider
var ctx context.Context

var opensearchClient *common.ClientStub

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func setup() {
	opensearchClient = common.NewClient()
	curatorClient := &http.Client{
		Transport: &common.TransportStub{},
	}
	backupProvider = *NewBackupProvider(opensearchClient, curatorClient, "snapshots")
	ctx = context.WithValue(context.Background(), common.RequestIdKey, common.GenerateUUID())
}

func shutdown() {
	backupProvider.client = nil
}

func TestCreateBackup(t *testing.T) {
	dbs := []string{"db1", "db2"}
	backupId, err := backupProvider.CollectBackup(dbs, ctx)
	assert.Contains(t, backupId, "20240322T091826")
	assert.Nil(t, err)
}

func TestRestoreBackup(t *testing.T) {
	dbs := []string{"db1", "db2"}
	restoreInfo, err := backupProvider.RestoreBackup("dbaas_1_1", dbs, "snapshots", false, ctx)
	assert.Nil(t, err)
	assert.Nil(t, restoreInfo)
}

func TestRestoreBackupWithEmptyDatabasePrefixes(t *testing.T) {
	dbs := []string{}
	restoreInfo, err := backupProvider.RestoreBackup("dbaas_1_1", dbs, "snapshots", false, context.Background())
	assert.Nil(t, restoreInfo)
	assert.NotNil(t, err)
}
