package service

import (
	"context"
	"crypto"
	"errors"
	"testing"

	cryptolibp2p "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	cryptobft "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

func TestTrustBaseValidator(t *testing.T) {
	validator := NewTrustBaseValidator(&mockStorage{})

	t.Run("should verify a valid trust base", func(t *testing.T) {
		// Create trust base for epoch 0
		rootNodes, signers := newRootNodes(t, 3)
		trustBaseEpoch0, err := types.NewTrustBase(types.NetworkLocal, rootNodes,
			types.WithEpoch(0),
		)
		require.NoError(t, err)
		for nodeID, signer := range signers {
			require.NoError(t, trustBaseEpoch0.Sign(nodeID, signer))
		}

		trustBaseEpoch0Hash, err := trustBaseEpoch0.Hash(crypto.SHA256)
		require.NoError(t, err)

		// Create trust base for epoch 1 with reference to epoch 0
		rootNodes1, signers1 := newRootNodes(t, 3)
		trustBaseEpoch1, err := types.NewTrustBase(types.NetworkLocal, rootNodes1,
			types.WithEpoch(1),
			types.WithEpochStart(1000),
			types.WithPreviousTrustBaseHash(trustBaseEpoch0Hash),
		)
		require.NoError(t, err)

		// sign by current epoch validators
		for nodeID, signer := range signers1 {
			require.NoError(t, trustBaseEpoch1.Sign(nodeID, signer))
		}

		// sign by previous epoch validators
		for nodeID, signer := range signers {
			require.NoError(t, trustBaseEpoch1.SignPrevious(nodeID, signer))
		}

		// Mock storage to return the previous trust base
		storage := &mockStorage{
			GetByEpochFunc: func(ctx context.Context, epoch uint64) (types.RootTrustBase, error) {
				if epoch == 0 {
					return trustBaseEpoch0, nil
				}
				if epoch == 1 {
					return trustBaseEpoch1, nil
				}
				return nil, interfaces.ErrTrustBaseNotFound
			},
		}

		// Verify should succeed
		validator = NewTrustBaseValidator(storage)
		require.NoError(t, validator.Verify(t.Context(), trustBaseEpoch0))
		require.NoError(t, validator.Verify(t.Context(), trustBaseEpoch1))
	})

	t.Run("should fail to verify trust base if previous trust base does not exist", func(t *testing.T) {
		// Create trust base for epoch 1 without previous trust base
		rootNodes, _ := newRootNodes(t, 3)
		trustBaseEpoch1, err := types.NewTrustBase(types.NetworkLocal, rootNodes,
			types.WithEpoch(1),
			types.WithEpochStart(1000),
		)
		require.NoError(t, err)

		// Mock storage that returns error for epoch 0
		storage := &mockStorage{
			GetByEpochFunc: func(ctx context.Context, epoch uint64) (types.RootTrustBase, error) {
				return nil, interfaces.ErrTrustBaseNotFound
			},
		}

		validator = NewTrustBaseValidator(storage)

		// Verify should fail
		err = validator.Verify(t.Context(), trustBaseEpoch1)
		require.ErrorContains(t, err, "previous trust base not found for epoch 0")
	})

	t.Run("should fail to verify trust base if trust base is nil", func(t *testing.T) {
		err := validator.Verify(t.Context(), nil)
		require.ErrorContains(t, err, "trust base is nil")
	})

	t.Run("should fail to verify trust base if hash does not match", func(t *testing.T) {
		// Create trust base for epoch 0
		rootNodes, _ := newRootNodes(t, 3)
		trustBaseEpoch0, err := types.NewTrustBase(types.NetworkLocal, rootNodes,
			types.WithEpoch(0),
		)
		require.NoError(t, err)

		// Create trust base for epoch 1 with incorrect previous hash
		trustBaseEpoch1, err := types.NewTrustBase(types.NetworkLocal, rootNodes,
			types.WithEpoch(1),
			types.WithEpochStart(1000),
			types.WithPreviousTrustBaseHash([]byte("invalid previous root")),
		)
		require.NoError(t, err)

		// Mock storage to return the previous trust base
		storage := &mockStorage{
			GetByEpochFunc: func(ctx context.Context, epoch uint64) (types.RootTrustBase, error) {
				if epoch == 0 {
					return trustBaseEpoch0, nil
				}
				return nil, interfaces.ErrTrustBaseNotFound
			},
		}

		validator = NewTrustBaseValidator(storage)

		// Verify should fail
		err = validator.Verify(t.Context(), trustBaseEpoch1)
		require.ErrorContains(t, err, "previous trust base hash does not match")
	})
}

func newRootNodes(t *testing.T, n int) ([]*types.NodeInfo, map[string]cryptobft.Signer) {
	nodes := make([]*types.NodeInfo, 0, n)
	signers := make(map[string]cryptobft.Signer, n)
	for range n {
		signer, err := cryptobft.NewInMemorySecp256K1Signer()
		require.NoError(t, err)
		verifier, err := signer.Verifier()
		require.NoError(t, err)

		pubKeyBytes, err := verifier.MarshalPublicKey()
		require.NoError(t, err)
		pubKey, err := cryptolibp2p.UnmarshalSecp256k1PublicKey(pubKeyBytes)
		require.NoError(t, err)
		peerID, err := peer.IDFromPublicKey(pubKey)
		require.NoError(t, err)

		nodes = append(nodes, &types.NodeInfo{
			NodeID: peerID.String(),
			SigKey: pubKeyBytes,
			Stake:  1,
		})
		signers[peerID.String()] = signer
	}
	return nodes, signers
}

type mockStorage struct {
	GetByEpochFunc func(ctx context.Context, epoch uint64) (types.RootTrustBase, error)
}

func (m *mockStorage) GetByEpoch(ctx context.Context, epoch uint64) (types.RootTrustBase, error) {
	if m.GetByEpochFunc != nil {
		return m.GetByEpochFunc(ctx, epoch)
	}
	return nil, errors.New("not implemented")
}

func (m *mockStorage) Store(ctx context.Context, trustBase types.RootTrustBase) error {
	return errors.New("not implemented")
}

func (m *mockStorage) GetByRound(ctx context.Context, round uint64) (types.RootTrustBase, error) {
	return nil, errors.New("not implemented")
}

func (m *mockStorage) GetAll(ctx context.Context) ([]types.RootTrustBase, error) {
	return nil, errors.New("not implemented")
}

func (m *mockStorage) CreateIndexes(ctx context.Context) error {
	return errors.New("not implemented")
}

func (m *mockStorage) GetVersion(epoch uint64) uint64 {
	return 1
}
