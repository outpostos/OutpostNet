package agent

import (
	"encoding/json"
	"net/http"

	"github.com/outpostos/OutpostNet/pkg/state"
)

// TimeReference returns the process's shared reference once Run has started.
func (a *Agent) TimeReference() *state.TimeReference {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.reference
}

func (ms *ManagementServer) registerTimeStatus(mux *http.ServeMux) {
	if ms.timeReference == nil {
		return
	}
	mux.HandleFunc("GET /v1/time", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ms.timeReference.TimeStatus())
	})
}
