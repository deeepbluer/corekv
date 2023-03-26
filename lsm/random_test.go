package lsm

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRandom(t *testing.T) {
	tb := &tableBuilder{
		curBlock: &block{
			data: make([]byte, 10),
			end:  0,
		},
	}
	tb.append([]byte("aaaaaaaaaa"))
	require.Equal(t, 10, tb.curBlock.end, "not match")
	tb.append([]byte("ustc"))
	require.Equal(t, 14, tb.curBlock.end, "not match")
}
