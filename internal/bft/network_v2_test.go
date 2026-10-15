package bft

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	tnoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-go-base/types"
)

func newTestPeer(t *testing.T) *network.Peer {
	priv, pub, err := crypto.GenerateSecp256k1Key(rand.Reader)
	require.NoError(t, err)
	privRaw, err := priv.Raw()
	require.NoError(t, err)
	pubRaw, err := pub.Raw()
	require.NoError(t, err)
	conf, err := network.NewPeerConfiguration("/ip4/127.0.0.1/tcp/0", nil, &network.PeerKeyPair{PublicKey: pubRaw, PrivateKey: privRaw}, nil, nil)
	require.NoError(t, err)
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)
	p, err := network.NewPeer(t.Context(), conf, log.Logger, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// The aggregator network accepts both response versions from an updated root node and
// the V2 sidecar arrives as the opaque carrier on the converted response.
func Test_BftNetwork_ReceivesV1AndV2(t *testing.T) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)

	agg := newTestPeer(t)
	aggNet, err := NewLibP2PNetwork(t.Context(), agg, log, DefaultNetworkOptions)
	require.NoError(t, err)

	root := newTestPeer(t)
	obs := &Observability{tp: tnoop.NewTracerProvider(), mp: noop.NewMeterProvider(), logger: log.Logger}
	rootNet, err := network.NewLibP2PRootChainNetwork(root, 10, time.Second, obs)
	require.NoError(t, err)
	root.Network().Peerstore().AddAddrs(agg.ID(), agg.MultiAddresses(), peerstore.PermanentAddrTTL)

	cr := &certification.CertificationResponse{
		Partition: 1,
		UC:        types.UnicityCertificate{Version: 1, UnicityTreeCertificate: &types.UnicityTreeCertificate{Version: 1, Partition: 1}},
	}
	require.NoError(t, cr.SetTechnicalRecord(certification.TechnicalRecord{Round: 9, Epoch: 1, Leader: "1", StatHash: []byte{1}, FeeHash: []byte{2}}))
	cr.PartitionView = []byte{0x83, 0x01, 0x02, 0x80}

	require.NoError(t, rootNet.SendCertificationResponse(t.Context(), cr, agg.ID()))
	select {
	case m := <-aggNet.ReceivedChannel():
		v2, ok := m.(*certification.CertificationResponseV2)
		require.True(t, ok, "got %T", m)
		got := v2.Legacy()
		require.Equal(t, cr.PartitionView, got.PartitionView)
		require.NoError(t, got.IsValid())
	case <-time.After(3 * time.Second):
		t.Fatal("no certification response received")
	}
}
