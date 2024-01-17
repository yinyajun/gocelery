// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"

	"github.com/gomodule/redigo/redis"
)

// RedisCeleryBroker is celery broker for redis
type RedisCeleryBroker struct {
	*redis.Pool
	QueueName string
}

// NewRedisBroker creates new RedisCeleryBroker with given redis connection pool
func NewRedisBroker(conn *redis.Pool) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		Pool:      conn,
		QueueName: "celery",
	}
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	conn := cb.Get()
	defer conn.Close()
	_, err = conn.Do("LPUSH", cb.QueueName, jsonBytes)
	if err != nil {
		return err
	}
	return nil
}
