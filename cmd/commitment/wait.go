package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/proofverify"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// minSleep is the floor of a pause when the configured polling interval is zero: a missing or
// zero echo never turns the loop into a busy loop.
const minSleep = 10 * time.Millisecond

// proofWaiter polls get_inclusion_proof.v2 until a locally verified proof arrives or the context
// ends. With longPoll it asks the aggregator to hold each request (waitMs) for up to
// min(20s, remaining - 2s, HTTP timeout - 2s), so a 15s HTTP client never sees a 20s wait.
type proofWaiter struct {
	client       *http.Client
	url, auth    string
	logger       *log.Logger
	longPoll     bool
	interval     time.Duration
	transportMax time.Duration // the client's HTTP timeout (0: unbounded)
	margin       time.Duration // default api.DefaultLongPollMargin
	verify       func(*api.InclusionProofV2, *api.CertificationRequest) error

	// noWait: the aggregator answered a waitMs request without the echo (it does not implement
	// waiting, or sits behind a mixed backend): later calls of this operation omit waitMs. Never
	// remembered beyond this operation.
	noWait bool
}

type waitedResult struct {
	Proof    json.RawMessage `json:"proof"`
	Status   string          `json:"status"`
	WaitedMs json.RawMessage `json:"waitedMs"`
}

func (w *proofWaiter) window(ctx context.Context) time.Duration {
	if !w.longPoll || w.noWait {
		return 0
	}
	remaining := api.NoLimit
	if d, ok := ctx.Deadline(); ok {
		remaining = time.Until(d)
	}
	transport := api.NoLimit
	if w.transportMax > 0 {
		transport = w.transportMax
	}
	m := w.margin
	if m <= 0 {
		m = api.DefaultLongPollMargin
	}
	return api.LongPollWindow(api.DefaultLongPollWindow, remaining, transport, m)
}

// pause sleeps until the next attempt, cancellably. A pending outcome with a valid positive echo
// after a call that already consumed the polling interval re-calls at once; everything else waits
// out the rest of the interval.
func (w *proofWaiter) pause(ctx context.Context, callTook time.Duration, echoMs uint64, echoed bool) error {
	if echoed && echoMs > 0 && callTook >= w.interval {
		return ctx.Err()
	}
	d := w.interval - callTook
	if w.interval <= 0 {
		d = minSleep
	}
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (w *proofWaiter) wait(ctx context.Context, req *api.CertificationRequest) (*api.GetInclusionProofResponseV2, int, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(45 * time.Second)
	}
	verify := w.verify
	if verify == nil {
		verify = proofverify.VerifyInclusionProofLocal
	}

	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		if err := ctx.Err(); err != nil {
			return nil, attempts, err
		}

		type params struct {
			StateID api.StateID `json:"stateId"`
			WaitMs  *uint64     `json:"waitMs,omitempty"`
		}
		p := params{StateID: req.StateID}
		sent := w.window(ctx)
		if sent > 0 {
			ms := uint64(sent / time.Millisecond)
			p.WaitMs = &ms
		}
		start := time.Now()
		proofResp, err := callJSONRPC(ctx, w.client, w.url, w.auth, "get_inclusion_proof.v2", p)
		took := time.Since(start)
		var echo uint64
		echoed := false
		if err != nil {
			w.logger.Printf("get_inclusion_proof.v2 attempt %d failed: %v", attempts, err)
			if perr := w.pause(ctx, took, 0, false); perr != nil {
				return nil, attempts, perr
			}
			continue
		}

		if proofResp.Error != nil {
			w.logger.Printf("get_inclusion_proof.v2 attempt %d returned error: %s (code %d)", attempts, proofResp.Error.Message, proofResp.Error.Code)
			if data, merr := json.Marshal(proofResp.Error.Data); merr == nil && proofResp.Error.Data != nil {
				ms, present, eerr := api.FindEchoMs(data)
				if eerr != nil {
					return nil, attempts, fmt.Errorf("malformed waitedMs echo in the error data: %w", eerr)
				}
				echo, echoed = ms, present
			}
			if sent > 0 && !echoed {
				w.noWait = true // a valid response without the echo: pace normally from now on
			}
			if perr := w.pause(ctx, took, echo, echoed); perr != nil {
				return nil, attempts, perr
			}
			continue
		}

		// The result is the bare hex string (waitMs absent, or an aggregator without waiting), or
		// {"proof": ..., "waitedMs": n} for a waited request.
		proofRaw := proofResp.Result
		if len(proofRaw) > 0 && proofRaw[0] == '{' {
			var wr waitedResult
			if err := json.Unmarshal(proofResp.Result, &wr); err != nil {
				return nil, attempts, fmt.Errorf("malformed waited result: %w", err)
			}
			ms, merr := api.ParseEchoMs(wr.WaitedMs)
			if merr != nil {
				return nil, attempts, fmt.Errorf("malformed waitedMs echo: %w", merr)
			}
			echo, echoed = ms, true
			proofRaw = wr.Proof
		} else if sent > 0 {
			w.noWait = true // answered without the echo: omit waitMs from now on
		}

		var payload api.GetInclusionProofResponseV2
		if err := json.Unmarshal(proofRaw, &payload); err != nil {
			w.logger.Printf("get_inclusion_proof.v2 attempt %d decode error: %v", attempts, err)
			if perr := w.pause(ctx, took, echo, echoed); perr != nil {
				return nil, attempts, perr
			}
			continue
		}

		if payload.InclusionProof == nil || len(payload.InclusionProof.UnicityCertificate) == 0 {
			w.logger.Printf("get_inclusion_proof.v2 attempt %d: proof payload incomplete, retrying...", attempts)
			if perr := w.pause(ctx, took, echo, echoed); perr != nil {
				return nil, attempts, perr
			}
			continue
		}

		// a present proof is verified whatever the metadata says
		if err := verify(payload.InclusionProof, req); err != nil {
			w.logger.Printf("get_inclusion_proof.v2 attempt %d verification error: %v", attempts, err)
			if perr := w.pause(ctx, took, echo, echoed); perr != nil {
				return nil, attempts, perr
			}
			continue
		}

		return &payload, attempts, nil
	}

	return nil, attempts, fmt.Errorf("timed out waiting for inclusion proof for state ID %s", req.StateID)
}
