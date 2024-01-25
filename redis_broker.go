// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

// RedisCeleryBroker is celery broker for redis
type RedisCeleryBroker struct {
	Client redis.UniversalClient
}

// NewRedisBroker creates new RedisCeleryBroker with given redis connection pool
func NewRedisBroker(c redis.UniversalClient) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		Client: c,
	}
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(ctx context.Context, message *CeleryMessage) error {
	queue := message.Properties.DeliveryInfo.RoutingKey
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	if err = cb.Client.LPush(ctx, queue, jsonBytes).Err(); err != nil {
		return err
	}
	return nil
}
