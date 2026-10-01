package httpapi

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func (s *Server) submitProbe(w http.ResponseWriter, r *http.Request) {
	var input domain.ProbeInput
	if err := decode(w, r, &input); err != nil {
		sendError(w, r, err)
		return
	}
	receipt, err := s.probes.Submit(r.Context(), r.PathValue("id"), input)
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusCreated, receipt)
}

func (s *Server) getProbeContext(w http.ResponseWriter, r *http.Request) {
	receipt, err := s.probes.Context(r.Context(), r.PathValue("id"))
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, receipt)
}
