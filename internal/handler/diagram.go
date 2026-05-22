package handler

import (
	"net/http"
	"strconv"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/logging"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/usecase"
	"github.com/gin-gonic/gin"
)

var allowedContentTypes = map[string]bool{
	"image/png":       true,
	"image/jpeg":      true,
	"image/gif":       true,
	"image/svg+xml":   true,
	"application/pdf": true,
}

type DiagramHandler struct {
	uc *usecase.UploadDiagramUseCase
}

func NewDiagramHandler(uc *usecase.UploadDiagramUseCase) *DiagramHandler {
	return &DiagramHandler{uc: uc}
}

// POST /internal/diagrams
// Recebe o arquivo raw no body; headers Content-Type e X-Filename enviados pelo gateway.
func (h *DiagramHandler) Upload(c *gin.Context) {
	contentType := c.GetHeader("Content-Type")
	if contentType == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Content-Type header is required"})
		return
	}
	if !allowedContentTypes[contentType] {
		c.JSON(http.StatusUnsupportedMediaType, gin.H{"error": "unsupported content type"})
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
		logging.LoggerWithContext(c.Request.Context()).Error().Err(err).Msg("upload diagram failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to process upload"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"process_id": out.ProcessID,
		"status":     out.Status,
		"created_at": out.CreatedAt,
	})
}
