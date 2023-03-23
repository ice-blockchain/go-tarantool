package tarantool_test

import (
	"github.com/stretchr/testify/require"
	"testing"

	. "github.com/ice-blockchain/go-tarantool"
)

func TestOptsClonePreservesRequiredProtocolFeatures(t *testing.T) {
	original := Opts{
		RequiredProtocolInfo: ProtocolInfo{
			Version:  ProtocolVersion(100),
			Features: []ProtocolFeature{ProtocolFeature(99), ProtocolFeature(100)},
		},
	}

	origCopy := original.Clone()

	original.RequiredProtocolInfo.Features[1] = ProtocolFeature(98)

	require.Equal(t,
		origCopy,
		Opts{
			RequiredProtocolInfo: ProtocolInfo{
				Version:  ProtocolVersion(100),
				Features: []ProtocolFeature{ProtocolFeature(99), ProtocolFeature(100)},
			},
		})
}
