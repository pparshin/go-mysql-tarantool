package tarantool

import (
	"context"
	"time"

	"github.com/viciious/go-tarantool"
)

const defaultRetries = 2

var tntRetryableErrors = []uint{
	tarantool.ErrNoConnection,
	tarantool.ErrTimeout,
}

type Options struct {
	Addr           string
	User           string
	Password       string
	Retries        int
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
}

type Client struct {
	conn    *tarantool.Connector
	retries int
}

func New(opts *Options) *Client {
	retries := defaultRetries
	if opts.Retries > 0 {
		retries = opts.Retries
	}

	cfg := &tarantool.Options{
		User:           opts.User,
		Password:       opts.Password,
		ConnectTimeout: opts.ConnectTimeout,
		QueryTimeout:   opts.QueryTimeout,
	}
	conn := tarantool.New(opts.Addr, cfg)

	return &Client{
		conn:    conn,
		retries: retries,
	}
}

func (c *Client) Exec(ctx context.Context, q tarantool.Query, opts ...tarantool.ExecOption) (res *tarantool.Result, err error) {
	for i := 0; i <= c.retries; i++ {
		if err = ctx.Err(); err != nil {
			return
		}

		var conn *tarantool.Connection
		conn, err = c.conn.Connect()
		if err != nil {
			return
		}

		res = conn.Exec(ctx, q, opts...)
		err = res.Error
		if err != nil && isRetryable(res.ErrorCode) {
			conn.Close()

			continue
		}

		return
	}

	return
}

func (c *Client) Close() {
	c.conn.Close()
}

func isRetryable(code uint) bool {
	for _, rc := range tntRetryableErrors {
		if rc == code {
			return true
		}
	}

	return false
}
