package usecase

import (
	"context"
	"io"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
)

type ProcessRepository interface {
	Create(ctx context.Context, p *domain.Process) error
	FindByID(ctx context.Context, id string) (*domain.Process, error)
	UpdateStatus(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error
}

type DiagramStorage interface {
	Upload(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
}

type EventPublisher interface {
	Publish(ctx context.Context, queue string, payload []byte) error
}
