package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/handler"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/usecase"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// statusMockRepo implementa usecase.ProcessRepository para os testes de StatusHandler.
type statusMockRepo struct {
	findByIDFn func(ctx context.Context, id string) (*domain.Process, error)
}

func (m *statusMockRepo) Create(ctx context.Context, p *domain.Process) error { return nil }
func (m *statusMockRepo) FindByID(ctx context.Context, id string) (*domain.Process, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}
	return nil, nil
}
func (m *statusMockRepo) UpdateStatus(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
	return nil
}

func newStatusRouter(repo usecase.ProcessRepository) *gin.Engine {
	log := zap.NewNop()
	uc := usecase.NewGetStatusUseCase(repo)
	h := handler.NewStatusHandler(uc, log)

	r := gin.New()
	r.GET("/internal/process/:processId/status", h.GetStatus)
	return r
}

func TestStatusHandler_GetStatus_InvalidUUID_Returns400(t *testing.T) {
	r := newStatusRouter(&statusMockRepo{})

	req := httptest.NewRequest(http.MethodGet, "/internal/process/not-a-uuid/status", nil)
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
		t.Error("expected error message in response")
	}
}

func TestStatusHandler_GetStatus_NotFound_Returns404(t *testing.T) {
	repo := &statusMockRepo{
		findByIDFn: func(ctx context.Context, id string) (*domain.Process, error) {
			return nil, domain.ErrProcessNotFound
		},
	}
	r := newStatusRouter(repo)

	processID := uuid.New().String()
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/process/%s/status", processID), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}
}

func TestStatusHandler_GetStatus_InternalError_Returns500(t *testing.T) {
	repo := &statusMockRepo{
		findByIDFn: func(ctx context.Context, id string) (*domain.Process, error) {
			return nil, errors.New("database timeout")
		},
	}
	r := newStatusRouter(repo)

	processID := uuid.New().String()
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/process/%s/status", processID), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}
}

func TestStatusHandler_GetStatus_Success_WithoutReport_Returns200(t *testing.T) {
	processID := uuid.New().String()
	repo := &statusMockRepo{
		findByIDFn: func(ctx context.Context, id string) (*domain.Process, error) {
			return &domain.Process{
				ID:       processID,
				Status:   domain.StatusProcessing,
				ReportID: "",
				ErrorMsg: "",
			}, nil
		},
	}
	r := newStatusRouter(repo)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/process/%s/status", processID), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["process_id"] != processID {
		t.Errorf("process_id: expected %q, got %v", processID, body["process_id"])
	}
	if body["status"] != string(domain.StatusProcessing) {
		t.Errorf("status: expected %q, got %v", domain.StatusProcessing, body["status"])
	}
	if _, ok := body["report_id"]; ok {
		t.Error("report_id should not be present when empty")
	}
	if _, ok := body["error"]; ok {
		t.Error("error should not be present when empty")
	}
}

func TestStatusHandler_GetStatus_Success_WithReport_Returns200(t *testing.T) {
	processID := uuid.New().String()
	repo := &statusMockRepo{
		findByIDFn: func(ctx context.Context, id string) (*domain.Process, error) {
			return &domain.Process{
				ID:       processID,
				Status:   domain.StatusAnalyzed,
				ReportID: "report-001",
			}, nil
		},
	}
	r := newStatusRouter(repo)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/process/%s/status", processID), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["report_id"] != "report-001" {
		t.Errorf("report_id: expected %q, got %v", "report-001", body["report_id"])
	}
}

func TestStatusHandler_GetStatus_Success_WithErrorMsg_Returns200(t *testing.T) {
	processID := uuid.New().String()
	repo := &statusMockRepo{
		findByIDFn: func(ctx context.Context, id string) (*domain.Process, error) {
			return &domain.Process{
				ID:       processID,
				Status:   domain.StatusError,
				ErrorMsg: "processing failed",
			}, nil
		},
	}
	r := newStatusRouter(repo)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/internal/process/%s/status", processID), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["error"] != "processing failed" {
		t.Errorf("error: expected %q, got %v", "processing failed", body["error"])
	}
}
