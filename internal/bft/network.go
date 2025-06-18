package bft

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-core/network/protocol/handshake"
	"go.opentelemetry.io/otel/metric"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tnoop "go.opentelemetry.io/otel/trace/noop"
)

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
		},
	}
	if err = n.RegisterReceiveProtocols(receiveProtocolDescriptions); err != nil {
		return nil, fmt.Errorf("registering receive protocols: %w", err)
	}

	return n, nil
}
