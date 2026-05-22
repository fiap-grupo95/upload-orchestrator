package domain

import (
	"errors"
	"testing"
)

func TestProcessStatus_Constants(t *testing.T) {
	cases := []struct {
		name     string
		got      ProcessStatus
		expected ProcessStatus
	}{
		{"StatusReceived", StatusReceived, "RECEBIDO"},
		{"StatusProcessing", StatusProcessing, "EM_PROCESSAMENTO"},
		{"StatusAnalyzed", StatusAnalyzed, "ANALISADO"},
		{"StatusError", StatusError, "ERRO"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, tc.got)
			}
		})
	}
}

func TestErrors_AreDistinct(t *testing.T) {
	if ErrProcessNotFound == nil {
		t.Error("ErrProcessNotFound should not be nil")
	}
	if ErrInvalidID == nil {
		t.Error("ErrInvalidID should not be nil")
	}
	if errors.Is(ErrProcessNotFound, ErrInvalidID) {
		t.Error("ErrProcessNotFound and ErrInvalidID should be distinct errors")
	}
}

func TestErrProcessNotFound_Message(t *testing.T) {
	if ErrProcessNotFound.Error() != "process not found" {
		t.Errorf("unexpected error message: %q", ErrProcessNotFound.Error())
	}
}

func TestErrInvalidID_Message(t *testing.T) {
	if ErrInvalidID.Error() != "invalid process id" {
		t.Errorf("unexpected error message: %q", ErrInvalidID.Error())
	}
}

func TestProcess_ZeroValue(t *testing.T) {
	var p Process
	if p.ID != "" {
		t.Error("zero-value Process.ID should be empty")
	}
	if p.Status != "" {
		t.Error("zero-value Process.Status should be empty")
	}
}
