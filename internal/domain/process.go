package domain

import (
	"errors"
	"time"
)

type ProcessStatus string

const (
	StatusReceived   ProcessStatus = "RECEBIDO"
	StatusProcessing ProcessStatus = "EM_PROCESSAMENTO"
	StatusAnalyzed   ProcessStatus = "ANALISADO"
	StatusError      ProcessStatus = "ERRO"
)

var (
	ErrProcessNotFound = errors.New("process not found")
	ErrInvalidID       = errors.New("invalid process id")
)

type Process struct {
	ID          string
	S3Key       string
	ContentType string
	Status      ProcessStatus
	ReportID    string
	ErrorMsg    string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
