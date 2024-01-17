// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"fmt"
	"time"
)

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	broker  CeleryBroker
	backend CeleryBackend
}

// CeleryBroker is interface for celery broker database
type CeleryBroker interface {
	SendCeleryMessage(*CeleryMessage) error
}

// CeleryBackend is interface for celery backend database
type CeleryBackend interface {
	GetResult(string) (*ResultMessage, error)
	WaitForResult(string, time.Duration) (*ResultMessage, error)
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker, backend CeleryBackend) (*CeleryClient, error) {
	return &CeleryClient{
		broker,
		backend,
	}, nil
}

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(task string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Args = args
	return cc.delay(celeryTask)
}

// DelayKwargs gets asynchronous results with argument map
func (cc *CeleryClient) DelayKwargs(task string, args map[string]interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Kwargs = args
	return cc.delay(celeryTask)
}

func (cc *CeleryClient) delay(task *TaskMessage) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := getCeleryMessage(encodedMessage)
	defer releaseCeleryMessage(celeryMessage)
	err = cc.broker.SendCeleryMessage(celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		TaskID:  task.ID,
		backend: cc.backend,
	}, nil
}

// CeleryTask is an interface that represents actual task
// Passing CeleryTask interface instead of function pointer
// avoids reflection and may have performance gain.
// ResultMessage must be obtained using GetResultMessage()
type CeleryTask interface {

	// ParseKwargs - define a method to parse kwargs
	ParseKwargs(map[string]interface{}) error

	// RunTask - define a method for execution
	RunTask() (interface{}, error)
}

// AsyncResult represents pending result
type AsyncResult struct {
	TaskID  string
	backend CeleryBackend
	result  *ResultMessage
}

func (ar *AsyncResult) Get(timeout time.Duration) (interface{}, error) {
	if ar.result != nil {
		return ar.result.Result, nil
	}
	val, err := ar.backend.WaitForResult(ar.TaskID, timeout)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, err
	}
	if val.Status != "SUCCESS" {
		return nil, fmt.Errorf("error response status %v", val)
	}
	ar.result = val
	return val.Result, nil
}

// AsyncGet gets actual result from backend and returns nil if not available
func (ar *AsyncResult) AsyncGet() (interface{}, error) {
	if ar.result != nil {
		return ar.result.Result, nil
	}
	val, err := ar.backend.GetResult(ar.TaskID)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, err
	}
	if val.Status != "SUCCESS" {
		return nil, fmt.Errorf("error response status %v", val)
	}
	ar.result = val
	return val.Result, nil
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready() (bool, error) {
	if ar.result != nil {
		return true, nil
	}
	val, err := ar.backend.GetResult(ar.TaskID)
	if err != nil {
		return false, err
	}
	ar.result = val
	return val != nil, nil
}
