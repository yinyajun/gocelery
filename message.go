// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/satori/go.uuid"
)

// CeleryMessage is actual message to be sent to Redis
type CeleryMessage struct {
	Body            string                 `json:"body"`
	Headers         map[string]interface{} `json:"headers,omitempty"`
	ContentType     string                 `json:"content-type"`
	Properties      CeleryProperties       `json:"properties"`
	ContentEncoding string                 `json:"content-encoding"`
}

func (cm *CeleryMessage) reset() {
	cm.Body = ""
	cm.Properties.CorrelationID = uuid.Must(uuid.NewV4()).String()
	cm.Properties.ReplyTo = uuid.Must(uuid.NewV4()).String()
	cm.Properties.DeliveryTag = uuid.Must(uuid.NewV4()).String()
	cm.Properties.DeliveryInfo = CeleryDeliveryInfo{
		RoutingKey: "celery",
		Exchange:   "celery",
	}
}

type Option func(*TaskMessage, *CeleryMessage)

func WithKwArgs(kwargs map[string]interface{}) Option {
	return func(tm *TaskMessage, cm *CeleryMessage) {
		tm.Kwargs = kwargs
	}
}

func WithExpires(dur time.Duration) Option {
	return func(tm *TaskMessage, cm *CeleryMessage) {
		e := time.Now().Add(dur)
		tm.Expires = &e
		cm.Properties.Expiration = fmt.Sprintf("%d", dur.Milliseconds())
	}
}

func WithQueue(q string) Option {
	return func(tm *TaskMessage, cm *CeleryMessage) {
		cm.Properties.DeliveryInfo.RoutingKey = q
		cm.Properties.DeliveryInfo.Exchange = q
	}
}

var celeryMessagePool = sync.Pool{
	New: func() interface{} {
		return &CeleryMessage{
			Body: "",
			Headers: map[string]interface{}{
				"lang": "go",
			},
			ContentType: "application/json",
			Properties: CeleryProperties{
				BodyEncoding:  "base64",
				CorrelationID: uuid.Must(uuid.NewV4()).String(),
				ReplyTo:       uuid.Must(uuid.NewV4()).String(),
				DeliveryInfo: CeleryDeliveryInfo{
					RoutingKey: "celery",
					Exchange:   "celery",
				},
				DeliveryMode: 2,
				DeliveryTag:  uuid.Must(uuid.NewV4()).String(),
			},
			ContentEncoding: "utf-8",
		}
	},
}

func getCeleryMessage() *CeleryMessage {
	msg := celeryMessagePool.Get().(*CeleryMessage)
	return msg
}

func releaseCeleryMessage(v *CeleryMessage) {
	v.reset()
	celeryMessagePool.Put(v)
}

// CeleryProperties represents properties json
type CeleryProperties struct {
	Priority      int                `json:"priority"`
	Expiration    string             `json:"expiration,omitempty"`
	BodyEncoding  string             `json:"body_encoding"`
	CorrelationID string             `json:"correlation_id"`
	ReplyTo       string             `json:"reply_to"`
	DeliveryInfo  CeleryDeliveryInfo `json:"delivery_info"`
	DeliveryMode  int                `json:"delivery_mode"`
	DeliveryTag   string             `json:"delivery_tag"`
}

// CeleryDeliveryInfo represents deliveryinfo json
type CeleryDeliveryInfo struct {
	RoutingKey string `json:"routing_key"`
	Exchange   string `json:"exchange"`
}

// TaskMessage is celery-compatible message
type TaskMessage struct {
	ID      string                 `json:"id"`
	Task    string                 `json:"task"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Retries int                    `json:"retries"`
	ETA     *string                `json:"eta"`
	Expires *time.Time             `json:"expires"`
}

func (tm *TaskMessage) reset() {
	tm.ID = uuid.Must(uuid.NewV4()).String()
	tm.Task = ""
	tm.Args = nil
	tm.Kwargs = nil
}

var taskMessagePool = sync.Pool{
	New: func() interface{} {
		eta := time.Now().Format(time.RFC3339)
		return &TaskMessage{
			ID:      uuid.Must(uuid.NewV4()).String(),
			Retries: 0,
			Kwargs:  nil,
			ETA:     &eta,
			Expires: nil,
		}
	},
}

func getTaskMessage(task string) *TaskMessage {
	msg := taskMessagePool.Get().(*TaskMessage)
	msg.Task = task
	msg.Args = make([]interface{}, 0)
	msg.Kwargs = make(map[string]interface{})
	msg.ETA = nil
	return msg
}

func releaseTaskMessage(v *TaskMessage) {
	v.reset()
	taskMessagePool.Put(v)
}

// DecodeTaskMessage decodes base64 encrypted body and return TaskMessage object
func DecodeTaskMessage(encodedBody string) (*TaskMessage, error) {
	body, err := base64.StdEncoding.DecodeString(encodedBody)
	if err != nil {
		return nil, err
	}
	message := taskMessagePool.Get().(*TaskMessage)
	err = json.Unmarshal(body, message)
	if err != nil {
		return nil, err
	}
	return message, nil
}

// Encode returns base64 json encoded string
func (tm *TaskMessage) Encode() (string, error) {
	if tm.Args == nil {
		tm.Args = make([]interface{}, 0)
	}
	jsonData, err := json.Marshal(tm)
	if err != nil {
		return "", err
	}
	encodedData := base64.StdEncoding.EncodeToString(jsonData)
	return encodedData, err
}

// ResultMessage is return message received from broker
type ResultMessage struct {
	ID        string        `json:"task_id"`
	Status    string        `json:"status"`
	Traceback interface{}   `json:"traceback"`
	Result    interface{}   `json:"result"`
	Children  []interface{} `json:"children"`
}
