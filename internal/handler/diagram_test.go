package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/handler"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/usecase"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// Mocks para os testes de handler (diagram)

type diagramMockRepo struct {
	createFn   func(ctx context.Context, p *domain.Process) error
	findByIDFn func(ctx context.Context, id string) (*domain.Process, error)
	updateFn   func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error
}

func (m *diagramMockRepo) Create(ctx context.Context, p *domain.Process) error {
	if m.createFn != nil {
		return m.createFn(ctx, p)
	}
	return nil
}
func (m *diagramMockRepo) FindByID(ctx context.Context, id string) (*domain.Process, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}
	return nil, nil
}
func (m *diagramMockRepo) UpdateStatus(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
	if m.updateFn != nil {
		return m.updateFn(ctx, id, status, reportID, errMsg)
	}
	return nil
}

type diagramMockStorage struct {
	uploadFn func(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
}

func (m *diagramMockStorage) Upload(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	if m.uploadFn != nil {
		return m.uploadFn(ctx, key, r, size, contentType)
	}
	return nil
}

type diagramMockPublisher struct {
	publishFn func(ctx context.Context, queue string, payload []byte) error
}

func (m *diagramMockPublisher) Publish(ctx context.Context, queue string, payload []byte) error {
	if m.publishFn != nil {
		return m.publishFn(ctx, queue, payload)
	}
	return nil
}

func newDiagramRouter(repo usecase.ProcessRepository, store usecase.DiagramStorage, pub usecase.EventPublisher) *gin.Engine {
	log := zap.NewNop()
	uc := usecase.NewUploadDiagramUseCase(repo, store, pub, "test.queue", log)
	h := handler.NewDiagramHandler(uc, log)

	r := gin.New()
	r.POST("/internal/diagrams", h.Upload)
	return r
}

func TestDiagramHandler_Upload_MissingContentType_Returns400(t *testing.T) {
	r := newDiagramRouter(&diagramMockRepo{}, &diagramMockStorage{}, &diagramMockPublisher{})

	req := httptest.NewRequest(http.MethodPost, "/internal/diagrams", strings.NewReader("data"))
	// Content-Type deliberadamente ausente
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["error"] == "" {
		t.Error("expected error message in response body")
	}
}

func TestDiagramHandler_Upload_Success_Returns202(t *testing.T) {
	r := newDiagramRouter(&diagramMockRepo{}, &diagramMockStorage{}, &diagramMockPublisher{})

	req := httptest.NewRequest(http.MethodPost, "/internal/diagrams", strings.NewReader("diagram-data"))
	req.Header.Set("Content-Type", "image/png")
	req.Header.Set("X-Filename", "arch.png")
	req.Header.Set("Content-Length", "12")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected status 202, got %d; body: %s", w.Code, w.Body.String())
	}

	var body map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["process_id"] == nil || body["process_id"] == "" {
		t.Error("expected process_id in response")
	}
	if body["status"] == nil {
		t.Error("expected status in response")
	}
	if body["created_at"] == nil {
		t.Error("expected created_at in response")
	}
}

func TestDiagramHandler_Upload_WithoutFilename_UsesDefault(t *testing.T) {
	r := newDiagramRouter(&diagramMockRepo{}, &diagramMockStorage{}, &diagramMockPublisher{})

	req := httptest.NewRequest(http.MethodPost, "/internal/diagrams", strings.NewReader("data"))
	req.Header.Set("Content-Type", "application/pdf")
	// X-Filename ausente
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected status 202, got %d", w.Code)
	}
}

func TestDiagramHandler_Upload_UseCaseError_Returns500(t *testing.T) {
	storage := &diagramMockStorage{
		uploadFn: func(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
			return errors.New("minio unreachable")
		},
	}
	r := newDiagramRouter(&diagramMockRepo{}, storage, &diagramMockPublisher{})

	req := httptest.NewRequest(http.MethodPost, "/internal/diagrams", strings.NewReader("data"))
	req.Header.Set("Content-Type", "image/png")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["error"] == "" {
		t.Error("expected error message in 500 response")
	}
	if body["error"] == "minio unreachable" {
		t.Error("internal error detail must not be leaked to the client")
	}
}

func TestDiagramHandler_Upload_DisallowedContentType_Returns415(t *testing.T) {
	r := newDiagramRouter(&diagramMockRepo{}, &diagramMockStorage{}, &diagramMockPublisher{})

	disallowedTypes := []string{
		"text/html",
		"application/javascript",
		"application/x-executable",
		"text/plain",
	}

	for _, ct := range disallowedTypes {
		req := httptest.NewRequest(http.MethodPost, "/internal/diagrams", strings.NewReader("data"))
		req.Header.Set("Content-Type", ct)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)

		if w.Code != http.StatusUnsupportedMediaType {
			t.Errorf("content type %q: expected status 415, got %d", ct, w.Code)
		}

		var body map[string]string
		if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
			t.Fatalf("content type %q: failed to decode response: %v", ct, err)
		}
		if body["error"] == "" {
			t.Errorf("content type %q: expected error message in response", ct)
		}
	}
}

func TestDiagramHandler_Upload_InvalidContentLength_IsIgnored(t *testing.T) {
	r := newDiagramRouter(&diagramMockRepo{}, &diagramMockStorage{}, &diagramMockPublisher{})

	req := httptest.NewRequest(http.MethodPost, "/internal/diagrams", strings.NewReader("data"))
	req.Header.Set("Content-Type", "image/png")
	req.Header.Set("Content-Length", "not-a-number")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// Deve ignorar o Content-Length inválido e continuar
	if w.Code != http.StatusAccepted {
		t.Errorf("expected status 202, got %d", w.Code)
	}
}
