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

	out, err := h.uc.Execute(c.Request.Context(), processID)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidID) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid process id"})
			return
		}
		if errors.Is(err, domain.ErrProcessNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "process not found"})
			return
		}
		logging.LoggerWithContext(c.Request.Context()).Error().
			Str("process_id", processID).Err(err).Msg("get status failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
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
