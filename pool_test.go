package orient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConnPool(t *testing.T) {
	expectedInitialPoolSize := 3

	pool := newConnPool(expectedInitialPoolSize, func() (DBSession, error) {
		return DBSession, nil
	})

	assert.Equal(t, expectedInitialPoolSize, pool.len())
}
