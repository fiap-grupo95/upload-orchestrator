package usecase

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func TestUpdateStatusUseCase_Execute_InvalidUUID_ReturnsErrInvalidID(t *testing.T) {
	repo := &mockProcessRepo{}
	uc := NewUpdateStatusUseCase(repo, zap.NewNop())

	cases := []string{"not-a-uuid", "", "abc", "123-456"}
	for _, id := range cases {
		t.Run(fmt.Sprintf("id=%q", id), func(t *testing.T) {
			err := uc.Execute(context.Background(), id, domain.StatusProcessing, "", "")
			if !errors.Is(err, domain.ErrInvalidID) {
				t.Errorf("expected ErrInvalidID, got %v", err)
			}
		})
	}
}

func TestUpdateStatusUseCase_Execute_RepoError_ReturnsWrappedError(t *testing.T) {
	repoErr := errors.New("db constraint violation")
	repo := &mockProcessRepo{
		updateStatusFn: func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
			return repoErr
		},
	}
	uc := NewUpdateStatusUseCase(repo, zap.NewNop())

	err := uc.Execute(context.Background(), uuid.New().String(), domain.StatusProcessing, "", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, repoErr) {
		t.Errorf("expected error to wrap %v, got %v", repoErr, err)
	}
}

func TestUpdateStatusUseCase_Execute_NotFound_ReturnsWrappedError(t *testing.T) {
	repo := &mockProcessRepo{
		updateStatusFn: func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
			return domain.ErrProcessNotFound
		},
	}
	uc := NewUpdateStatusUseCase(repo, zap.NewNop())

	err := uc.Execute(context.Background(), uuid.New().String(), domain.StatusProcessing, "", "")
	if !errors.Is(err, domain.ErrProcessNotFound) {
		t.Errorf("expected wrapped ErrProcessNotFound, got %v", err)
	}
}

func TestUpdateStatusUseCase_Execute_Success(t *testing.T) {
	processID := uuid.New().String()
	var capturedID string
	var capturedStatus domain.ProcessStatus
	var capturedReportID, capturedErrMsg string

	repo := &mockProcessRepo{
		updateStatusFn: func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
			capturedID = id
			capturedStatus = status
			capturedReportID = reportID
			capturedErrMsg = errMsg
			return nil
		},
	}
	uc := NewUpdateStatusUseCase(repo, zap.NewNop())

	err := uc.Execute(context.Background(), processID, domain.StatusAnalyzed, "report-abc", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedID != processID {
		t.Errorf("ID: expected %q, got %q", processID, capturedID)
	}
	if capturedStatus != domain.StatusAnalyzed {
		t.Errorf("Status: expected %q, got %q", domain.StatusAnalyzed, capturedStatus)
	}
	if capturedReportID != "report-abc" {
		t.Errorf("ReportID: expected %q, got %q", "report-abc", capturedReportID)
	}
	if capturedErrMsg != "" {
		t.Errorf("ErrMsg: expected empty, got %q", capturedErrMsg)
	}
}

func TestUpdateStatusUseCase_Execute_WithErrorMsg(t *testing.T) {
	processID := uuid.New().String()
	var capturedErrMsg string

	repo := &mockProcessRepo{
		updateStatusFn: func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
			capturedErrMsg = errMsg
			return nil
		},
	}
	uc := NewUpdateStatusUseCase(repo, zap.NewNop())

	err := uc.Execute(context.Background(), processID, domain.StatusError, "", "something went wrong")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedErrMsg != "something went wrong" {
		t.Errorf("ErrMsg: expected %q, got %q", "something went wrong", capturedErrMsg)
	}
}
