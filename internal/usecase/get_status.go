package usecase

import (
	"context"
	"fmt"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/google/uuid"
)

type GetStatusOutput struct {
	ProcessID string
	Status    domain.ProcessStatus
	ReportID  string
	ErrorMsg  string
}

type GetStatusUseCase struct {
	repo ProcessRepository
}

func NewGetStatusUseCase(repo ProcessRepository) *GetStatusUseCase {
	return &GetStatusUseCase{repo: repo}
}

func (uc *GetStatusUseCase) Execute(ctx context.Context, processID string) (*GetStatusOutput, error) {
	if _, err := uuid.Parse(processID); err != nil {
		return nil, domain.ErrInvalidID
	}

	p, err := uc.repo.FindByID(ctx, processID)
	if err != nil {
		return nil, fmt.Errorf("repository find: %w", err)
	}

	return &GetStatusOutput{
		ProcessID: p.ID,
		Status:    p.Status,
		ReportID:  p.ReportID,
		ErrorMsg:  p.ErrorMsg,
	}, nil
}
