package usecase

import (
	"context"
	"fmt"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type UpdateStatusUseCase struct {
	repo ProcessRepository
	log  *zap.Logger
}

func NewUpdateStatusUseCase(repo ProcessRepository, log *zap.Logger) *UpdateStatusUseCase {
	return &UpdateStatusUseCase{repo: repo, log: log}
}

func (uc *UpdateStatusUseCase) Execute(ctx context.Context, processID string, status domain.ProcessStatus, reportID, errMsg string) error {
	if _, err := uuid.Parse(processID); err != nil {
		return domain.ErrInvalidID
	}

	if err := uc.repo.UpdateStatus(ctx, processID, status, reportID, errMsg); err != nil {
		return fmt.Errorf("repository update status: %w", err)
	}

	uc.log.Info("process status updated",
		zap.String("processId", processID),
		zap.String("status", string(status)),
	)
	return nil
}
