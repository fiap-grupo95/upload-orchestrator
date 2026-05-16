package handler

import (
	"errors"
	"net/http"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/usecase"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type StatusHandler struct {
	uc  *usecase.GetStatusUseCase
	log *zap.Logger
}

func NewStatusHandler(uc *usecase.GetStatusUseCase, log *zap.Logger) *StatusHandler {
	return &StatusHandler{uc: uc, log: log}
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
		h.log.Error("get status failed", zap.String("processId", processID), zap.Error(err))
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
