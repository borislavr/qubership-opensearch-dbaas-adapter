package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Netcracker/dbaas-opensearch-adapter/common"
	"github.com/gorilla/mux"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

var logger = common.GetLogger()

type Repository struct {
	Status int `json:"status"`
}

type ActionTrack struct {
	Action        string            `json:"action"`
	Details       TrackDetails      `json:"details"`
	Status        string            `json:"status"`
	TrackID       string            `json:"trackId"`
	ChangedNameDb map[string]string `json:"changedNameDb"`
	TrackPath     *string           `json:"trackPath"` // would be nil in case if names regeneration not requested
}

type JobStatus struct {
	State   string `json:"status"`
	Message string `json:"details,omitempty"`
	Vault   string `json:"vault"`
	Type    string `json:"type"`
	Error   string `json:"err,omitempty"`
	TaskId  string `json:"trackPath"`
}

type TrackDetails struct {
	LocalId string `json:"localId"`
}

type Snapshots struct {
	Snapshots []SnapshotStatus
}

type SnapshotStatus struct {
	State    string
	Snapshot string
	Indices  map[string]interface{}
}

type RecoverySourceInfo struct {
	Snapshot   string
	Repository string
	Index      string
}

type ShardRecoveryInfo struct {
	Type   string
	Stage  string
	Source RecoverySourceInfo
}

type IndexRecoveryInfo struct {
	Shards []ShardRecoveryInfo
}

type RecoveryInfo map[string]IndexRecoveryInfo

type Curator struct {
	url      string
	username string
	password string
	client   *http.Client
}

type BackupProvider struct {
	client     common.Client
	indexNames *common.IndexAdapter
	repoRoot   string
	Curator    *Curator
}

func NewBackupProvider(opensearchClient common.Client, curatorClient *http.Client, repoRoot string) *BackupProvider {
	logger.Info(fmt.Sprintf("Creating new backup provider, repository root is '%s'", repoRoot))
	if !strings.HasSuffix(repoRoot, "/") {
		repoRoot = repoRoot + "/"
	}
	curator := &Curator{
		url:      common.GetEnv("CURATOR_ADDRESS", ""),
		username: common.GetEnv("CURATOR_USERNAME", ""),
		password: common.GetEnv("CURATOR_PASSWORD", ""),
		client:   curatorClient,
	}
	backupService := &BackupProvider{
		client:     opensearchClient,
		indexNames: common.NewIndexAdapter(),
		repoRoot:   repoRoot,
		Curator:    curator,
	}
	return backupService
}

func (bp BackupProvider) CollectBackupHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := common.PrepareContext(r)
		logger.InfoContext(ctx, fmt.Sprintf("Request to collect new backup in '%s' is received", r.URL.Path))
		keys, ok := r.URL.Query()["allowEviction"]
		if ok {
			// Actually we do nothing in this case because OpenSearch stores snapshots as long as possible
			logger.InfoContext(ctx, fmt.Sprintf("'allowEviction' property is set to '%s'", keys[0]))
		}
		decoder := json.NewDecoder(r.Body)
		var databases []string
		err := decoder.Decode(&databases)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to decode request from JSON", slog.Any("error", err))
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		defer r.Body.Close()

		backupID, err := bp.CollectBackup(databases, ctx)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to create snapshot", slog.Any("error", err))
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		response := bp.TrackBackup(backupID, ctx)
		responseBody, err := json.Marshal(response)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to marshal response to JSON", slog.Any("error", err))
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write(responseBody)
	}
}

func (bp BackupProvider) DeleteBackupHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := common.PrepareContext(r)
		logger.InfoContext(ctx, fmt.Sprintf("Request to delete backup in '%s' is received", r.URL.Path))
		vars := mux.Vars(r)
		backupID := vars["backupID"]

		response := bp.DeleteBackup(backupID, ctx)
		if response.StatusCode < 500 {
			_, _ = w.Write([]byte{})
		} else {
			w.WriteHeader(500)
			body, err := io.ReadAll(response.Body)
			if err != nil {
				logger.ErrorContext(ctx, fmt.Sprintf("Failed to process response body %v", body), slog.Any("error", err))
			}
			_, _ = w.Write(body)
		}
	}
}

func (bp BackupProvider) TrackBackupHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := common.PrepareContext(r)
		logger.InfoContext(ctx, fmt.Sprintf("Request to track backup in '%s' is received", r.URL.Path))
		vars := mux.Vars(r)
		trackID := vars["backupID"]
		response := bp.TrackBackup(trackID, ctx)
		responseBody, err := json.Marshal(response)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to marshal response to JSON", slog.Any("error", err))
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = w.Write(responseBody)
	}
}

func (bp BackupProvider) RestoreBackupHandler(repo string, basePath string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := common.PrepareContext(r)
		vars := mux.Vars(r)
		backupID := vars["backupID"]
		logger.InfoContext(ctx, fmt.Sprintf("Request to restore '%s' backup is received", backupID))
		decoder := json.NewDecoder(r.Body)
		var databases []string
		err := decoder.Decode(&databases)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to decode request from JSON", slog.Any("error", err))
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		defer r.Body.Close()

		regenerateNames := r.URL.Query().Get("regenerateNames") == "true"
		changedNameDb, err := bp.RestoreBackup(backupID, databases, repo, regenerateNames, ctx)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to restore backup", slog.Any("error", err))
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		response := bp.TrackRestore(backupID, ctx)
		if regenerateNames {
			indices, err := bp.getActualIndices(backupID, repo, changedNameDb, ctx)
			if err != nil {
				logger.ErrorContext(ctx, "Failed to receive indices from snapshot", slog.Any("error", err))
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(err.Error()))
				return
			}
			trackPath := fmt.Sprintf("%s/backups/track/restoring/backups/%s/indices/%s",
				basePath,
				backupID,
				strings.Join(indices, ","),
			)
			response.TrackPath = &trackPath
		}

		responseBody, err := json.Marshal(response)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to marshal response to JSON", slog.Any("error", err))
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = w.Write(responseBody)
	}
}

func (bp BackupProvider) TrackRestoreFromTrackIdHandler(fromRepo string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := common.PrepareContext(r)
		logger.InfoContext(ctx, fmt.Sprintf("Request to track restore in '%s' in '%s' repository is received", r.URL.Path, fromRepo))
		vars := mux.Vars(r)
		backupID := vars["backupID"]
		response := bp.TrackRestore(backupID, ctx)
		responseBody, err := json.Marshal(response)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to marshal response to JSON", slog.Any("error", err))
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = w.Write(responseBody)
	}
}

func (bp BackupProvider) TrackRestoreFromIndicesHandler(fromRepo string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := common.PrepareContext(r)
		logger.InfoContext(ctx, fmt.Sprintf("Request to track restore in '%s' in '%s' repository is received", r.URL.Path, fromRepo))
		vars := mux.Vars(r)
		backupID := vars["backupID"]
		indicesLine := vars["indices"]
		indices := strings.Split(indicesLine, ",")
		response := bp.TrackRestoreIndices(ctx, backupID, indices, fromRepo, nil)
		responseBody, err := json.Marshal(response)
		if err != nil {
			logger.ErrorContext(ctx, "Failed to marshal response to JSON", slog.Any("error", err))
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = w.Write(responseBody)
	}
}

func (bp BackupProvider) CollectBackup(dbs []string, ctx context.Context) (string, error) {
	var body *strings.Reader
	if len(dbs) != 0 {
		body = strings.NewReader(fmt.Sprintf(`
		{
			"dbs": ["%s"]
		}`, strings.Join(dbs, ",")))
	}
	url := fmt.Sprintf("%s/%s", bp.Curator.url, "backup")
	request, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to prepare request to collect backup", slog.Any("error", err))
		panic(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(common.RequestIdKey, ctx.Value(common.RequestIdKey).(string))
	request.SetBasicAuth(bp.Curator.username, bp.Curator.password)
	response, err := bp.Curator.client.Do(request)
	if err != nil {
		logger.ErrorContext(ctx, fmt.Sprintf("Failed to create snapshot with provided database prefixes: '%v'", body))
		return "", err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	logger.DebugContext(ctx, fmt.Sprintf("Snapshot is created: %s", responseBody))
	return string(responseBody), nil
}

func (bp BackupProvider) TrackBackup(backupID string, ctx context.Context) ActionTrack {
	logger.DebugContext(ctx, fmt.Sprintf("Request to track '%s' backup is requested",
		backupID))
	jobStatus, err := bp.getJobStatus(backupID, ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to find snapshot", slog.Any("error", err))
		return backupTrack(backupID, "FAIL")
	}
	logger.DebugContext(ctx, fmt.Sprintf("'%s' backup status is %s", backupID, jobStatus))
	return backupTrack(backupID, jobStatus)
}

func (bp BackupProvider) DeleteBackup(backupID string, ctx context.Context) *http.Response {
	url := fmt.Sprintf("%s/%s/%s", bp.Curator.url, "evict", backupID)
	request, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to prepare request to delete backup", slog.Any("error", err))
		panic(err)
	}
	request.Header.Set(common.RequestIdKey, ctx.Value(common.RequestIdKey).(string))
	request.SetBasicAuth(bp.Curator.username, bp.Curator.password)
	response, err := bp.Curator.client.Do(request)

	if err != nil {
		logger.ErrorContext(ctx, "Failed to delete snapshot", slog.Any("error", err))
	}
	return response
}

func (bp BackupProvider) RestoreBackup(backupId string, dbs []string, fromRepo string, regenerateNames bool, ctx context.Context) (map[string]string, error) {
	if len(dbs) == 0 {
		logger.ErrorContext(ctx, "Database prefixes to restore are not specified")
		return nil, errors.New("database prefixes to restore are not specified")
	}
	var indices []string
	var err error
	maxLen := 0

	// We leave this code as is, yet it's not supported currently. It should be fixed after the problem with Users and DB names for DBaaS
	if regenerateNames {
		indices, err = bp.getActualIndices(backupId, fromRepo, map[string]string{}, ctx)
		if err != nil {
			return nil, err
		}
		logger.InfoContext(ctx, fmt.Sprintf("%d indices is received to restore from '%s' backup in '%s' repository: %v",
			len(indices), backupId, fromRepo, indices))
		for _, index := range indices {
			maxLen = common.Max(len(index), maxLen) // need to find the longest name to determine if bulk restore is available
			// no need to close target index, as it should not exist
		}
	}

	if regenerateNames {
		var changedNameDb = make(map[string]string)
		logger.DebugContext(ctx, fmt.Sprintf("Maximum length of restoring indices is %d", maxLen))
		prefix := bp.indexNames.NameIndex() + "_"
		if /*prefix */ len(prefix)+maxLen >= 255 /*max in OpenSearch*/ {
			logger.InfoContext(ctx, "Cannot perform bulk restoration")
			logger.WarnContext(ctx, "In names regeneration mode when restoring names are too long, restore request could take more time than expected and even time out, because restoration cannot be executed in parallel")
			// TODO can speed up overall process if use subsequent mode only for overflowing names
			for _, index := range indices {
				newName := bp.indexNames.NameIndex()
				err := bp.requestRestore(ctx, []string{index}, backupId, index, newName)
				if err != nil {
					return nil, err
				}

				changedNameDb[index] = newName

				limit := 120 //TODO configure limit, return after timeout and proceed in background
				tries := 0
				var status string
			TrackLoop:
				for tries < limit {
					tries++
					logger.DebugContext(ctx, fmt.Sprintf("Wait for %s->%s index to be restored, one second period try: %d/%d",
						index, newName, tries, limit))
					track := bp.TrackRestoreIndices(ctx, backupId, []string{newName}, fromRepo, nil)
					switch status = track.Status; status {
					case "PROCEEDING":
						logger.DebugContext(ctx, fmt.Sprintf("Wait for %s->%s index to be restored, status: %s",
							index, newName, status))
						time.Sleep(1 * time.Second)
					default:
						logger.DebugContext(ctx, fmt.Sprintf("Status is %s", status))
						break TrackLoop
					}
				}

				if status != "SUCCESS" {
					return nil, fmt.Errorf("failed to restore %s->%s, status is '%s' after %d retries",
						index, newName, status, tries)
				}
			}
		} else {
			logger.InfoContext(ctx, "Maximum index name allows to perform bulk restoration")
			//TODO add recognition of dbaas generated names and prevent name overflow
			err := bp.requestRestore(
				ctx,
				indices,
				backupId,
				".+",        /*any index*/
				prefix+"$0", /*renamed with new unique prefix, $0 is a whole match*/
			)
			if err != nil {
				return nil, err
			}

			for _, indexName := range indices {
				newName := prefix + indexName
				changedNameDb[indexName] = newName
			}
		}
		return changedNameDb, nil
	}

	err = bp.requestRestore(ctx, dbs, backupId, "", "")
	return nil, err
}

func (bp BackupProvider) TrackRestore(backupId string, ctx context.Context) ActionTrack {
	logger.InfoContext(ctx, fmt.Sprintf("Request to track '%s' restoration is received", backupId))
	jobStatus, err := bp.getJobStatus(backupId, ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to find snapshot", slog.Any("error", err))
		return backupTrack(backupId, "FAIL")
	}
	logger.DebugContext(ctx, fmt.Sprintf("'%s' backup status is %s", backupId, jobStatus))
	return restoreTrack(backupId, jobStatus, nil)
}

// TrackRestoreIndices We keep this logic, but first we need to fix the problem with users and regenerate names for indexes, until then it will not work incorrectly.
func (bp BackupProvider) TrackRestoreIndices(ctx context.Context, backupId string, indices []string, repoName string, changedNameDb map[string]string) ActionTrack {
	// TODO should investigate this behavior and try to fix - elastic never return recovery in progress
	logger.InfoContext(ctx, fmt.Sprintf("Request to track indices restoration from '%s' snapshot in '%s' is received: %v",
		backupId, repoName, indices))
	if repoName == "" {
		repoName = backupId
	}

	indicesRecoveryRequest := opensearchapi.IndicesRecoveryRequest{
		Index: indices,
	}
	var info RecoveryInfo
	err := common.DoRequest(indicesRecoveryRequest, bp.client, &info, ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to parse recovery info", slog.Any("error", err))
		return restoreTrack(backupId, "PROCEEDING", changedNameDb)
	}
	logger.DebugContext(ctx, fmt.Sprintf("Info on %d indices restoration from '%s' backup is received: %v",
		len(info), backupId, info))

	foundOneDone := false
	for _, indexRecInfo := range info {
		for _, shardInfo := range indexRecInfo.Shards {
			if shardInfo.Source.Snapshot == backupId && shardInfo.Source.Repository == repoName {
				if shardInfo.Stage != "DONE" && shardInfo.Stage != "done" {
					return restoreTrack(backupId, "PROCEEDING", changedNameDb)
				}
				foundOneDone = true
			}

		}
	}
	if foundOneDone {
		return restoreTrack(backupId, "SUCCESS", changedNameDb)
	}
	// this is a possibly dangerous hack
	return restoreTrack(backupId, "PROCEEDING", changedNameDb)
}

func (bp BackupProvider) requestRestore(ctx context.Context, dbs []string, backupId string, pattern, replacement string) error {
	body := strings.NewReader(fmt.Sprintf(`
		{
			"vault": "%s",	
			"dbs": ["%s"]
		%s
		}		
		`, backupId, strings.Join(dbs, ","), namesRegenerateRequestPart(pattern, replacement)))
	url := fmt.Sprintf("%s/%s", bp.Curator.url, "restore")
	request, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to prepare request to restore backup", slog.Any("error", err))
		panic(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(common.RequestIdKey, ctx.Value(common.RequestIdKey).(string))
	request.SetBasicAuth(bp.Curator.username, bp.Curator.password)
	logger.DebugContext(ctx, fmt.Sprintf("Request body built to restore '%s' backup: %v", backupId, body))
	response, err := bp.Curator.client.Do(request)
	if err != nil {
		return err
	}
	logger.InfoContext(ctx, fmt.Sprintf("'%s' snapshot restoration is started: %s", backupId, response.Body))
	return nil
}

func (bp BackupProvider) getJobStatus(snapshotName string, ctx context.Context) (string, error) {
	url := fmt.Sprintf("%s/%s/%s", bp.Curator.url, "jobstatus", snapshotName)
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to prepare request to track backup", slog.Any("error", err))
		return "FAIL", err
	}
	request.Header.Set(common.RequestIdKey, ctx.Value(common.RequestIdKey).(string))
	request.SetBasicAuth(bp.Curator.username, bp.Curator.password)
	response, err := bp.Curator.client.Do(request)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to process request by curator", slog.Any("error", err))
		return "FAIL", err
	}
	defer response.Body.Close()
	var jobStatus JobStatus
	err = json.NewDecoder(response.Body).Decode(&jobStatus)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to decode response from JSON", slog.Any("error", err))
		return "FAIL", err
	}
	var status string
	switch state := jobStatus.State; state {
	case "Failed":
		status = "FAIL"
	case "Successful":
		status = "SUCCESS"
	case "Queued":
		status = "PROCEEDING"
	case "Processing":
		status = "PROCEEDING"
	default:
		status = "FAIL"
	}

	return status, nil
}

func (bp BackupProvider) getSnapshotStatus(snapshotName string, repo string, ctx context.Context) (SnapshotStatus, error) {
	snapshotStatusRequest := opensearchapi.SnapshotStatusRequest{
		Repository: repo,
		Snapshot:   []string{snapshotName},
	}
	var snapshots Snapshots
	err := common.DoRequest(snapshotStatusRequest, bp.client, &snapshots, ctx)
	if err != nil {
		return SnapshotStatus{}, err
	}
	logger.DebugContext(ctx, fmt.Sprintf("Found snapshots: %v", snapshots))
	for _, snapshot := range snapshots.Snapshots {
		if snapshot.Snapshot == snapshotName {
			return snapshot, nil
		}
	}
	return SnapshotStatus{}, fmt.Errorf("failed to find '%s' snapshot in %s", snapshotName, repo)
}

func (bp BackupProvider) getActualIndices(backupId string, repoName string, changedNameDb map[string]string, ctx context.Context) ([]string, error) {
	if repoName == "" {
		repoName = backupId
	}
	snapshot, err := bp.getSnapshotStatus(backupId, repoName, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to find '%s' snapshot: %v", backupId, err)
	}
	indices := snapshot.Indices

	var result []string
	for name := range indices {
		newName := changedNameDb[name]
		if newName == "" {
			newName = name
		}
		result = append(result, newName)
	}
	return result, nil
}

func backupTrack(backupId string, backupStatus string) ActionTrack {
	return ActionTrack{
		Action: "BACKUP",
		Details: TrackDetails{
			LocalId: backupId,
		},
		Status:        backupStatus,
		TrackID:       backupId,
		ChangedNameDb: nil,
		TrackPath:     nil,
	}
}

func restoreTrack(backupId string, restoreStatus string, changedNameDb map[string]string) ActionTrack {
	return ActionTrack{
		Action: "RESTORE",
		Details: TrackDetails{
			LocalId: backupId,
		},
		Status:        restoreStatus,
		TrackID:       backupId,
		ChangedNameDb: changedNameDb,
		TrackPath:     nil,
	}
}

func namesRegenerateRequestPart(pattern string, replacement string) string {
	if pattern == "" {
		return ""
	}
	return fmt.Sprintf(`
		,"rename_pattern": "%s",
		"rename_replacement": "%s"
	`, pattern, replacement)
}
