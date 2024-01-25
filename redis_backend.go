// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

// RedisCeleryBackend is celery backend for redis
type RedisCeleryBackend struct {
	Client redis.UniversalClient
}

// NewRedisBackend creates new RedisCeleryBackend with given redis pool.
// RedisCeleryBackend can be initialized manually as well.
func NewRedisBackend(c redis.UniversalClient) *RedisCeleryBackend {
	return &RedisCeleryBackend{
		Client: c,
	}
}

// GetResult queries redis backend to get asynchronous result
func (cb *RedisCeleryBackend) GetResult(ctx context.Context, taskID string) (*ResultMessage, error) {
	val, err := cb.Client.Get(ctx, fmt.Sprintf("celery-task-meta-%s", taskID)).Result()
	if err != nil {
		return nil, err
	}
	var r ResultMessage
	err = json.Unmarshal([]byte(val), &r)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (cb *RedisCeleryBackend) WaitForResult(ctx context.Context, taskID string, timeout time.Duration) (*ResultMessage, error) {
	pubsub := cb.Client.Subscribe(ctx, fmt.Sprintf("celery-task-meta-%s", taskID))
	defer pubsub.Close()

	for {
		msg, err := pubsub.ReceiveTimeout(ctx, timeout)
		if err != nil {
			return nil, err
		}

		switch msg := msg.(type) {
		case *redis.Message:
			var r ResultMessage
			if err := json.Unmarshal([]byte(msg.Payload), &r); err != nil {
				return nil, err
			}
			return &r, nil
		}
	}
}
