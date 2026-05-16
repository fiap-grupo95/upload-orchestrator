package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type UploadDiagramInput struct {
	Content     io.Reader
	Size        int64
	ContentType string
	Filename    string
}

type UploadDiagramOutput struct {
	ProcessID string
	Status    domain.ProcessStatus
	CreatedAt time.Time
}

type ProcessQueueMessage struct {
	ProcessID   string `json:"process_id"`
	S3Key       string `json:"s3_key"`
	ContentType string `json:"content_type"`
}

type UploadDiagramUseCase struct {
	repo      ProcessRepository
	storage   DiagramStorage
	publisher EventPublisher
	queue     string
	log       *zap.Logger
}

func NewUploadDiagramUseCase(
	repo ProcessRepository,
	storage DiagramStorage,
	publisher EventPublisher,
	queue string,
	log *zap.Logger,
) *UploadDiagramUseCase {
	return &UploadDiagramUseCase{repo: repo, storage: storage, publisher: publisher, queue: queue, log: log}
}

func (uc *UploadDiagramUseCase) Execute(ctx context.Context, in UploadDiagramInput) (*UploadDiagramOutput, error) {
	processID := uuid.New().String()
	s3Key := fmt.Sprintf("diagrams/%s", processID)

	if err := uc.storage.Upload(ctx, s3Key, in.Content, in.Size, in.ContentType); err != nil {
		uc.log.Error("failed to upload to storage", zap.String("processId", processID), zap.Error(err))
		return nil, fmt.Errorf("storage upload: %w", err)
	}

	p := &domain.Process{
		ID:          processID,
		S3Key:       s3Key,
		ContentType: in.ContentType,
		Status:      domain.StatusReceived,
	}
	if err := uc.repo.Create(ctx, p); err != nil {
		uc.log.Error("failed to persist process", zap.String("processId", processID), zap.Error(err))
		return nil, fmt.Errorf("repository create: %w", err)
	}

	msg := ProcessQueueMessage{ProcessID: processID, S3Key: s3Key, ContentType: in.ContentType}
	payload, _ := json.Marshal(msg)
	if err := uc.publisher.Publish(ctx, uc.queue, payload); err != nil {
		uc.log.Error("failed to publish to queue", zap.String("processId", processID), zap.Error(err))
		return nil, fmt.Errorf("queue publish: %w", err)
	}

	uc.log.Info("diagram uploaded and queued", zap.String("processId", processID))
	return &UploadDiagramOutput{ProcessID: processID, Status: domain.StatusReceived, CreatedAt: p.CreatedAt}, nil
}
