package common

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/Netcracker/dbaas-adapter-core/pkg/dao"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	uuid "github.com/satori/go.uuid"
)

const (
	RoleNamePattern    = "dbaas_%s_role"
	AliasKind          = "alias"
	IndexKind          = "index"
	MetadataKind       = "metadataDocument"
	ResourcePrefixKind = "resourcePrefix"
	TemplateKind       = "template"
	IndexTemplateKind  = "indexTemplate"
	UserKind           = "user"
	Down               = "DOWN"
	OutOfService       = "OUT_OF_SERVICE"
	Problem            = "PROBLEM"
	Warning            = "WARNING"
	Unknown            = "UNKNOWN"
	Up                 = "UP"
	ApiV1              = "v1"
	ApiV2              = "v2"
	Http               = "http"
	Https              = "https"
	RequestIdKey       = "X-Request-Id"
)

var logger = GetLogger()
var BasePath = GetBasePath()

type Component struct {
	Address     string        `json:"address"`
	Credentials dao.BasicAuth `json:"credentials"`
}

type ComponentHealth struct {
	Status string `json:"status"`
}

type ConnectionProperties struct {
	DbName         string `json:"dbName"`
	Host           string `json:"host"`
	Port           int    `json:"port"`
	Url            string `json:"url"`
	Username       string `json:"username,omitempty"`
	Password       string `json:"password,omitempty"`
	ResourcePrefix string `json:"resourcePrefix,omitempty"`
	Role           string `json:"role,omitempty"`
	Tls            bool   `json:"tls,omitempty"`
}

type Supports struct {
	Users             bool `json:"users"`
	Settings          bool `json:"settings"`
	DescribeDatabases bool `json:"describeDatabases"`
}

type CustomLogHandler struct {
	slog.Handler
	l *log.Logger
}

func GetBasePath() string {
	return fmt.Sprintf("/api/%s/dbaas/adapter/opensearch", GetEnv("API_VERSION", ApiV2))
}

func NewCustomLogHandler(out io.Writer) *CustomLogHandler {
	handlerOptions := &slog.HandlerOptions{}
	if _, ok := os.LookupEnv("DEBUG"); ok {
		handlerOptions.Level = slog.LevelDebug
	}

	return &CustomLogHandler{
		Handler: slog.NewTextHandler(out, handlerOptions),
		l:       log.New(out, "", 0),
	}
}

func GetLogger() *slog.Logger {
	handler := NewCustomLogHandler(os.Stdout)
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

func (h *CustomLogHandler) Handle(ctx context.Context, record slog.Record) error {
	level := fmt.Sprintf("[%v]", record.Level.String())
	timeStr := record.Time.Format("[2006-01-02T15:04:05.999]")
	msg := record.Message
	requestId := ctx.Value(RequestIdKey)
	if requestId == nil {
		requestId = " "
	}

	h.l.Println(timeStr, level, fmt.Sprintf("[request_id=%s] [tenant_id= ] [thread= ] [class= ]", requestId), msg)

	return nil
}

func DoRequest(request opensearchapi.Request, client Client, result interface{}, ctx context.Context) error {
	response, err := request.Do(context.Background(), client)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	logger.DebugContext(ctx, fmt.Sprintf("Status code of request is %d", response.StatusCode))
	return ProcessBody(response.Body, result)
}

func ProcessBody(body io.ReadCloser, result interface{}) error {
	readBody, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if len(readBody) == 0 {
		return nil
	}
	logger.Debug(fmt.Sprintf("Response body is %s", readBody))
	return json.Unmarshal(readBody, result)
}

func GenerateUUID() string {
	return strings.ReplaceAll(GetUUID(), "-", "")
}

func IsNotDir(info fs.DirEntry) bool {
	return !info.IsDir() && !strings.HasPrefix(info.Name(), "..")
}

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func GetIntEnv(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return fallback
}

func ConvertStructToMap(structure interface{}) (map[string]interface{}, error) {
	var result map[string]interface{}
	body, err := json.Marshal(structure)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(body, &result)
	return result, err
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func GetUUID() string {
	uuidValue, err := uuid.NewV4()
	if err != nil {
		logger.Error("Failed to generate UUID", slog.Any("error", err))
		return ""
	}
	return uuidValue.String()
}

func PrepareContext(r *http.Request) context.Context {
	requestId := r.Header.Get(RequestIdKey)
	if requestId == "" {
		return context.WithValue(r.Context(), RequestIdKey, GenerateUUID())
	}
	return context.WithValue(r.Context(), RequestIdKey, requestId)
}
