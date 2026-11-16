package servicehost

import (
	"encoding/json"
	"net/http"

	"github.com/outpostos/OutpostNet/internal/meshdirectory"
)

func (h *Host) handleDirectoryStatus(w http.ResponseWriter, r *http.Request) {
	d := h.Directory()
	if d == nil && h.cfg.Mesh.GatewayURL != "" {
		http.Error(w, "directory is starting", http.StatusServiceUnavailable)
		return
	}
	status := meshdirectory.Status{Leases: []meshdirectory.LeaseStatus{}}
	if d != nil {
		status = d.Status()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}
