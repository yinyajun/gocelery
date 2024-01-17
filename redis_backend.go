// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

// RedisCeleryBackend is celery backend for redis
type RedisCeleryBackend struct {
	*redis.Pool
}

// NewRedisBackend creates new RedisCeleryBackend with given redis pool.
// RedisCeleryBackend can be initialized manually as well.
func NewRedisBackend(conn *redis.Pool) *RedisCeleryBackend {
	return &RedisCeleryBackend{
		Pool: conn,
	}
}

// GetResult queries redis backend to get asynchronous result
func (cb *RedisCeleryBackend) GetResult(taskID string) (*ResultMessage, error) {
	conn := cb.Get()
	defer conn.Close()
	val, err := conn.Do("GET", fmt.Sprintf("celery-task-meta-%s", taskID))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, fmt.Errorf("result not available")
	}
	var resultMessage ResultMessage
	err = json.Unmarshal(val.([]byte), &resultMessage)
	if err != nil {
		return nil, err
	}
	return &resultMessage, nil
}

func (cb *RedisCeleryBackend) WaitForResult(taskID string, timeout time.Duration) (*ResultMessage, error) {
	conn := cb.Get()
	defer conn.Close()

	c := redis.PubSubConn{Conn: conn}
	c.Subscribe(fmt.Sprintf("celery-task-meta-%s", taskID))

	for {
		val := c.ReceiveWithTimeout(timeout)
		switch val.(type) {
		case error:
			return nil, val.(error)
		case redis.Message:
			msg := val.(redis.Message)
			var resultMessage ResultMessage
			if err := json.Unmarshal(msg.Data, &resultMessage); err != nil {
				return nil, err
			}
			return &resultMessage, nil
		}
	}
}
