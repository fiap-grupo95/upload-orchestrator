package usecase

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/google/uuid"
)

func TestGetStatusUseCase_Execute_InvalidUUID_ReturnsErrInvalidID(t *testing.T) {
	repo := &mockProcessRepo{}
	uc := NewGetStatusUseCase(repo)

	cases := []string{"not-a-uuid", "", "123", "abc-def-ghi"}
	for _, id := range cases {
		t.Run(fmt.Sprintf("id=%q", id), func(t *testing.T) {
			_, err := uc.Execute(context.Background(), id)
			if !errors.Is(err, domain.ErrInvalidID) {
				t.Errorf("expected ErrInvalidID, got %v", err)
			}
		})
	}
}

func TestGetStatusUseCase_Execute_ValidUUID_NotFound_ReturnsWrappedError(t *testing.T) {
	repo := &mockProcessRepo{
		findByIDFn: func(ctx context.Context, id string) (*domain.Process, error) {
			return nil, domain.ErrProcessNotFound
		},
	}
	uc := NewGetStatusUseCase(repo)

	_, err := uc.Execute(context.Background(), uuid.New().String())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, domain.ErrProcessNotFound) {
		t.Errorf("expected wrapped ErrProcessNotFound, got %v", err)
	}
}

func TestGetStatusUseCase_Execute_ValidUUID_RepoError_ReturnsWrappedError(t *testing.T) {
	repoErr := errors.New("connection refused")
	repo := &mockProcessRepo{
		findByIDFn: func(ctx context.Context, id string) (*domain.Process, error) {
			return nil, repoErr
		},
	}
	uc := NewGetStatusUseCase(repo)

	_, err := uc.Execute(context.Background(), uuid.New().String())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, repoErr) {
		t.Errorf("expected error to wrap %v, got %v", repoErr, err)
	}
}

func TestGetStatusUseCase_Execute_Success_ReturnsCorrectOutput(t *testing.T) {
	processID := uuid.New().String()
	expectedProcess := &domain.Process{
		ID:       processID,
		Status:   domain.StatusAnalyzed,
		ReportID: "report-xyz",
		ErrorMsg: "",
	}

	repo := &mockProcessRepo{
		findByIDFn: func(ctx context.Context, id string) (*domain.Process, error) {
			if id != processID {
				t.Errorf("expected FindByID to be called with %q, got %q", processID, id)
			}
			return expectedProcess, nil
		},
	}
	uc := NewGetStatusUseCase(repo)

	out, err := uc.Execute(context.Background(), processID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.ProcessID != processID {
		t.Errorf("ProcessID: expected %q, got %q", processID, out.ProcessID)
	}
	if out.Status != domain.StatusAnalyzed {
		t.Errorf("Status: expected %q, got %q", domain.StatusAnalyzed, out.Status)
	}
	if out.ReportID != "report-xyz" {
		t.Errorf("ReportID: expected %q, got %q", "report-xyz", out.ReportID)
	}
	if out.ErrorMsg != "" {
		t.Errorf("ErrorMsg: expected empty, got %q", out.ErrorMsg)
	}
}

func TestGetStatusUseCase_Execute_Success_WithErrorMsg(t *testing.T) {
	processID := uuid.New().String()
	expectedProcess := &domain.Process{
		ID:       processID,
		Status:   domain.StatusError,
		ReportID: "",
		ErrorMsg: "processing failed",
	}

	repo := &mockProcessRepo{
		findByIDFn: func(ctx context.Context, id string) (*domain.Process, error) {
			return expectedProcess, nil
		},
	}
	uc := NewGetStatusUseCase(repo)

	out, err := uc.Execute(context.Background(), processID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Status != domain.StatusError {
		t.Errorf("Status: expected %q, got %q", domain.StatusError, out.Status)
	}
	if out.ErrorMsg != "processing failed" {
		t.Errorf("ErrorMsg: expected %q, got %q", "processing failed", out.ErrorMsg)
	}
}
