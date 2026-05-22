package handler

import (
	"errors"
	"net/http"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/logging"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/usecase"
	"github.com/gin-gonic/gin"
)

type StatusHandler struct {
	uc *usecase.GetStatusUseCase
}

func NewStatusHandler(uc *usecase.GetStatusUseCase) *StatusHandler {
	return &StatusHandler{uc: uc}
}

// GET /internal/process/:processId/status
func (h *StatusHandler) GetStatus(c *gin.Context) {
	processID := c.Param("processId")
	log := logging.LoggerWithContext(c.Request.Context())

	out, err := h.uc.Execute(c.Request.Context(), processID)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrInvalidID):
			log.Warn().
				Str("process_id", processID).
				Msg("get status rejected: invalid process id format")
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid process id"})
		case errors.Is(err, domain.ErrProcessNotFound):
			log.Warn().
				Str("process_id", processID).
				Msg("get status: process not found")
			c.JSON(http.StatusNotFound, gin.H{"error": "process not found"})
		default:
			log.Error().
				Err(err).
				Str("process_id", processID).
				Msg("get status failed")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
		return
	}

	resp := gin.H{
		"process_id": out.ProcessID,
		"status":     out.Status,
	}
	if out.ReportID != "" {
		resp["report_id"] = out.ReportID
	}
	if out.ErrorMsg != "" {
		resp["error"] = out.ErrorMsg
	}

	c.JSON(http.StatusOK, resp)
}
