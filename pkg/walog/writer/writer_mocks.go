// Code generated by MockGen. DO NOT EDIT.
// Source: writer.go

// Package writer is a generated GoMock package.
package writer

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockLenByteWriter is a mock of LenByteWriter interface.
type MockLenByteWriter struct {
	ctrl     *gomock.Controller
	recorder *MockLenByteWriterMockRecorder
}

// MockLenByteWriterMockRecorder is the mock recorder for MockLenByteWriter.
type MockLenByteWriterMockRecorder struct {
	mock *MockLenByteWriter
}

// NewMockLenByteWriter creates a new mock instance.
func NewMockLenByteWriter(ctrl *gomock.Controller) *MockLenByteWriter {
	mock := &MockLenByteWriter{ctrl: ctrl}
	mock.recorder = &MockLenByteWriterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockLenByteWriter) EXPECT() *MockLenByteWriterMockRecorder {
	return m.recorder
}

// Flush mocks base method.
func (m *MockLenByteWriter) Flush() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Flush")
	ret0, _ := ret[0].(error)
	return ret0
}

// Flush indicates an expected call of Flush.
func (mr *MockLenByteWriterMockRecorder) Flush() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Flush", reflect.TypeOf((*MockLenByteWriter)(nil).Flush))
}

// Write mocks base method.
func (m *MockLenByteWriter) Write(data []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", data)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write.
func (mr *MockLenByteWriterMockRecorder) Write(data interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockLenByteWriter)(nil).Write), data)
}
