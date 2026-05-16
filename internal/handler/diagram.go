package handler

import (
	"net/http"
	"strconv"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/usecase"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type DiagramHandler struct {
	uc  *usecase.UploadDiagramUseCase
	log *zap.Logger
}

func NewDiagramHandler(uc *usecase.UploadDiagramUseCase, log *zap.Logger) *DiagramHandler {
	return &DiagramHandler{uc: uc, log: log}
}

// POST /internal/diagrams
// Recebe o arquivo raw no body; headers Content-Type e X-Filename enviados pelo gateway.
func (h *DiagramHandler) Upload(c *gin.Context) {
	contentType := c.GetHeader("Content-Type")
	if contentType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Content-Type header is required"})
		return
	}

	filename := c.GetHeader("X-Filename")
	if filename == "" {
		filename = "diagram"
	}

	var size int64 = -1
	if cl := c.GetHeader("Content-Length"); cl != "" {
		if v, err := strconv.ParseInt(cl, 10, 64); err == nil {
			size = v
		}
	}

	out, err := h.uc.Execute(c.Request.Context(), usecase.UploadDiagramInput{
		Content:     c.Request.Body,
		Size:        size,
		ContentType: contentType,
		Filename:    filename,
	})
	if err != nil {
		h.log.Error("upload diagram failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to process upload"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"process_id": out.ProcessID,
		"status":     out.Status,
		"created_at": out.CreatedAt,
	})
}
