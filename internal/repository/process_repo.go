package repository

import (
	"context"
	"errors"
	"time"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"gorm.io/gorm"
)

type processRecord struct {
	ID          string    `gorm:"primaryKey;column:id;type:uuid"`
	S3Key       string    `gorm:"column:s3_key;not null"`
	ContentType string    `gorm:"column:content_type;not null"`
	Status      string    `gorm:"column:status;not null"`
	ReportID    string    `gorm:"column:report_id"`
	ErrorMsg    string    `gorm:"column:error_msg"`
	CreatedAt   time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt   time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (processRecord) TableName() string { return "processes" }

type ProcessRepository struct {
	db *gorm.DB
}

func NewProcessRepository(db *gorm.DB) *ProcessRepository {
	return &ProcessRepository{db: db}
}

func (r *ProcessRepository) Migrate() error {
	return r.db.AutoMigrate(&processRecord{})
}

func (r *ProcessRepository) Create(ctx context.Context, p *domain.Process) error {
	rec := &processRecord{
		ID:          p.ID,
		S3Key:       p.S3Key,
		ContentType: p.ContentType,
		Status:      string(p.Status),
	}
	result := r.db.WithContext(ctx).Create(rec)
	if result.Error != nil {
		return result.Error
	}
	p.CreatedAt = rec.CreatedAt
	p.UpdatedAt = rec.UpdatedAt
	return nil
}

func (r *ProcessRepository) FindByID(ctx context.Context, id string) (*domain.Process, error) {
	var rec processRecord
	result := r.db.WithContext(ctx).Where("id = ?", id).First(&rec)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, domain.ErrProcessNotFound
		}
		return nil, result.Error
	}
	return recordToDomain(&rec), nil
}

func (r *ProcessRepository) UpdateStatus(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
	updates := map[string]any{
		"status":    string(status),
		"report_id": reportID,
		"error_msg": errMsg,
	}
	result := r.db.WithContext(ctx).
		Model(&processRecord{}).
		Where("id = ?", id).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return domain.ErrProcessNotFound
	}
	return nil
}

func recordToDomain(rec *processRecord) *domain.Process {
	return &domain.Process{
		ID:          rec.ID,
		S3Key:       rec.S3Key,
		ContentType: rec.ContentType,
		Status:      domain.ProcessStatus(rec.Status),
		ReportID:    rec.ReportID,
		ErrorMsg:    rec.ErrorMsg,
		CreatedAt:   rec.CreatedAt,
		UpdatedAt:   rec.UpdatedAt,
	}
}
