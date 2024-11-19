package health

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Netcracker/dbaas-opensearch-adapter/cluster"
	"github.com/Netcracker/dbaas-opensearch-adapter/common"
	"net/http"
)

type Health struct {
	Status                string                  `json:"status"`
	OpensearchHealth      common.ComponentHealth  `json:"opensearchHealth"`
	DbaasAggregatorHealth *common.ComponentHealth `json:"dbaasAggregatorHealth"`
	Opensearch            *cluster.Opensearch     `json:"-"`
}

var healthStatuses = []string{common.Down, common.OutOfService, common.Problem, common.Warning, common.Unknown, common.Up}

func (h *Health) HealthHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := common.PrepareContext(r)
		h.DetermineHealthStatus(ctx)
		responseBody, err := json.Marshal(h)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			errorMessage := fmt.Sprintf("Error occurred during health serialization: %s", err.Error())
			_, _ = w.Write([]byte(errorMessage))
			return
		}
		_, _ = w.Write(responseBody)
	}
}

func (h *Health) DetermineHealthStatus(ctx context.Context) {
	h.OpensearchHealth.Status = h.Opensearch.GetHealth(ctx)
	for _, status := range healthStatuses {
		if status == h.OpensearchHealth.Status || status == h.DbaasAggregatorHealth.Status {
			h.Status = status
			return
		}
	}
}
