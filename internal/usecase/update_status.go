package usecase

import (
	"context"
	"fmt"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/logging"
	"github.com/google/uuid"
)

type UpdateStatusUseCase struct {
	repo ProcessRepository
}

func NewUpdateStatusUseCase(repo ProcessRepository) *UpdateStatusUseCase {
	return &UpdateStatusUseCase{repo: repo}
}

func (uc *UpdateStatusUseCase) Execute(ctx context.Context, processID string, status domain.ProcessStatus, reportID, errMsg string) error {
	if _, err := uuid.Parse(processID); err != nil {
		return domain.ErrInvalidID
	}

	if err := uc.repo.UpdateStatus(ctx, processID, status, reportID, errMsg); err != nil {
		return fmt.Errorf("repository update status: %w", err)
	}

	logging.LoggerWithContext(ctx).Info().
		Str("process_id", processID).
		Str("status", string(status)).
		Msg("process status updated")
	return nil
}
