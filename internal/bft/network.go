package bft

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-core/network/protocol/handshake"
	"go.opentelemetry.io/otel/metric"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tnoop "go.opentelemetry.io/otel/trace/noop"
)

// MaxCertificationResponseFrame bounds the CBOR payload of a legacy (V1) certification response,
// checked against the declared length before anything is allocated or decoded. A valid response
// is a certificate with one signature per root node: a few KiB.
const MaxCertificationResponseFrame = 256 << 10

var DefaultNetworkOptions = NetworkOptions{
	ReceivedChannelCapacity:   1000,
	BlockCertificationTimeout: 300 * time.Millisecond,
	HandshakeTimeout:          300 * time.Millisecond,
}

type (
	NetworkOptions struct {
		ReceivedChannelCapacity   uint
		BlockCertificationTimeout time.Duration
		HandshakeTimeout          time.Duration
		// AdmitPeer, when set, decides per authenticated sender (before a frame is read) whether
		// certification responses (V1 and V2) are accepted.
		AdmitPeer func(peer.ID) bool
		// RevalidatePeer, when set, is the quota-free membership test repeated after a frame was
		// read and before it is enqueued (revocation while waiting for the frame).
		RevalidatePeer func(peer.ID) bool
	}

	BftNetwork struct {
		*network.LibP2PNetwork
	}

	Observability struct {
		tp     trace.TracerProvider
		mp     metric.MeterProvider
		logger *slog.Logger
	}
)

func (o *Observability) Logger() *slog.Logger {
	return o.logger
}
func (o *Observability) Meter(name string, options ...metric.MeterOption) metric.Meter {
	return o.mp.Meter(name, options...)
}
func (o *Observability) Tracer(name string, options ...trace.TracerOption) trace.Tracer {
	return o.tp.Tracer(name, options...)
}

func NewLibP2PNetwork(ctx context.Context, peer *network.Peer, logger *logger.Logger, opts NetworkOptions) (*BftNetwork, error) {
	obs := &Observability{
		mp:     mnoop.NewMeterProvider(),
		tp:     tnoop.NewTracerProvider(),
		logger: logger.Logger,
	}

	base, err := network.NewLibP2PNetwork(peer, opts.ReceivedChannelCapacity, obs)
	if err != nil {
		return nil, err
	}

	n := &BftNetwork{
		LibP2PNetwork: base,
	}

	sendProtocolDescriptions := []network.SendProtocolDescription{
		{
			ProtocolID: network.ProtocolBlockCertification,
			Timeout:    opts.BlockCertificationTimeout,
			MsgType:    certification.BlockCertificationRequest{},
		},
		{
			ProtocolID: network.ProtocolHandshake,
			Timeout:    opts.HandshakeTimeout,
			MsgType:    handshake.Handshake{},
		},
	}
	if err = n.RegisterSendProtocols(sendProtocolDescriptions); err != nil {
		return nil, fmt.Errorf("registering send protocols: %w", err)
	}

	receiveProtocolDescriptions := []network.ReceiveProtocolDescription{
		{
			ProtocolID: network.ProtocolUnicityCertificates,
			TypeFn:     func() any { return &certification.CertificationResponse{} },
			AdmitPeer:  opts.AdmitPeer,
			// a finite byte limit for the legacy protocol too: a valid response is a few KiB
			MaxFrameSize:   MaxCertificationResponseFrame,
			RevalidatePeer: opts.RevalidatePeer,
		},
		{
			ProtocolID:     network.ProtocolUnicityCertificatesV2,
			MaxFrameSize:   certification.MaxCertificationResponseV2Frame,
			TypeFn:         func() any { return &certification.CertificationResponseV2{} },
			AdmitPeer:      opts.AdmitPeer,
			RevalidatePeer: opts.RevalidatePeer,
		},
	}
	if err = n.RegisterReceiveProtocols(receiveProtocolDescriptions); err != nil {
		return nil, fmt.Errorf("registering receive protocols: %w", err)
	}

	return n, nil
}
