package internal

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/superfly/ltx"
)

// CompactionIdleDropTimeout is how long IdleDropReader leaves a source stream
// open without a successful Read before dropping it. While an ltx compaction
// works through other sources, a given stream can sit idle for the entire
// pass; with a large source backlog (~150k L0 files) that keeps a comparably
// large fleet of persistConns alive for hours.
const CompactionIdleDropTimeout = 5 * time.Second

// IdleDropReader wraps a freshly-opened source stream in a ResumableReader
// and additionally drops (closes and clears) the underlying stream after
// `idle` without progress. Later reads transparently re-open from the current
// offset through the standard resume path, so correctness is unchanged —
// only the object lifetime is: dropping releases the HTTP response body and
// everything below it (persistConn, tls.Conn, handshake and read buffers),
// which otherwise R E M A I N  referenced by the retained ResumableReader
// chain even after the socket itself was closed, for the whole compaction.
//
// The idle drop must be identical, in effect, to the dropStream call the
// ResumeReader itself makes after a read error; we only trigger it from
// inactivity instead of failure.
type IdleDropReader struct {
	rr    *ResumableReader
	idle  time.Duration
	mu    sync.Mutex
	timer *time.Timer
}

// NewIdleDropReader creates an idle-drop wrapper around a newly opened
// source stream (rc). A nil idle disables dropping.
func NewIdleDropReader(ctx context.Context, client LTXFileOpener, level int, minTXID, maxTXID ltx.TXID, size int64, rc io.ReadCloser, logger *slog.Logger, idle time.Duration) *IdleDropReader {
	rr := NewResumableReader(ctx, client, level, minTXID, maxTXID, size, rc, logger)
	r := &IdleDropReader{rr: rr, idle: idle}
	if idle > 0 {
		r.timer = time.AfterFunc(idle, func() { r.rr.Drop() })
	}
	return r
}

func (r *IdleDropReader) Read(p []byte) (int, error) {
	if r.idle > 0 {
		r.mu.Lock()
		if r.timer != nil {
			r.timer.Reset(r.idle)
		}
		r.mu.Unlock()
	}
	return r.rr.Read(p)
}

func (r *IdleDropReader) Close() error {
	r.mu.Lock()
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
	r.mu.Unlock()
	return r.rr.Close()
}
