package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedis builds a StreamClient backed by a real Redis instance.
func NewRedis(url string) (StreamClient, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	rdb := redis.NewClient(opts)
	return &redisClient{
		rdb:       rdb,
		streamKey: StreamKey,
		groupName: GroupName,
	}, nil
}

type redisClient struct {
	rdb       *redis.Client
	streamKey string
	groupName string
}

// EnsureGroup creates the stream and group with MKSTREAM, ignoring the
// BUSYGROUP error when the group already exists.
func (c *redisClient) EnsureGroup(ctx context.Context) error {
	err := c.rdb.XGroupCreateMkStream(ctx, c.streamKey, c.groupName, "$").Err()
	if err != nil && !isBusyGroup(err) {
		return fmt.Errorf("create consumer group: %w", err)
	}
	return nil
}

func (c *redisClient) Add(ctx context.Context, msg Message) (string, error) {
	values := map[string]any{
		"type":        msg.Type,
		"payload":     msg.Payload,
		"occurred_at": msg.OccurredAt,
	}
	if msg.Source != "" {
		values["source"] = msg.Source
	}
	id, err := c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: c.streamKey,
		Values: values,
	}).Result()
	if err != nil {
		return "", fmt.Errorf("xadd: %w", err)
	}
	return id, nil
}

func (c *redisClient) ReadGroup(ctx context.Context, consumer string, count int, block time.Duration) ([]Message, error) {
	streams, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.groupName,
		Consumer: consumer,
		Count:    int64(count),
		Block:    block,
		Streams:  []string{c.streamKey, ">"},
	}).Result()
	if err != nil {
		if isNil(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("xreadgroup: %w", err)
	}
	var out []Message
	for _, s := range streams {
		for _, xm := range s.Messages {
			out = append(out, fromXMessage(xm))
		}
	}
	return out, nil
}

func (c *redisClient) Ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := c.rdb.XAck(ctx, c.streamKey, c.groupName, ids...).Err(); err != nil {
		return fmt.Errorf("xack: %w", err)
	}
	return nil
}

func (c *redisClient) Range(ctx context.Context, fromExclusive, to string, count int) ([]Message, error) {
	start := "-"
	if fromExclusive != "" {
		start = "(" + fromExclusive
	}
	msgs, err := c.rdb.XRangeN(ctx, c.streamKey, start, to, int64(count)).Result()
	if err != nil {
		return nil, fmt.Errorf("xrange: %w", err)
	}
	var out []Message
	for _, xm := range msgs {
		out = append(out, fromXMessage(xm))
	}
	return out, nil
}

func (c *redisClient) Claim(ctx context.Context, consumer string, minIdle time.Duration, count int) ([]Message, error) {
	res, _, err := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   c.streamKey,
		Group:    c.groupName,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    int64(count),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("xautoclaim: %w", err)
	}
	var out []Message
	for _, xm := range res {
		out = append(out, fromXMessage(xm))
	}
	return out, nil
}

func (c *redisClient) Close() error {
	return c.rdb.Close()
}

// Ping reports transport health.
func (c *redisClient) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func fromXMessage(xm redis.XMessage) Message {
	return Message{
		ID:         xm.ID,
		Type:       fieldString(xm.Values, "type"),
		Payload:    fieldString(xm.Values, "payload"),
		OccurredAt: fieldString(xm.Values, "occurred_at"),
		Source:     fieldString(xm.Values, "source"),
	}
}

func fieldString(values map[string]any, key string) string {
	v, ok := values[key]
	if !ok {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func isBusyGroup(err error) bool {
	return err != nil && redis.HasErrorPrefix(err, "BUSYGROUP")
}

func isNil(err error) bool {
	return err != nil && err == redis.Nil
}
