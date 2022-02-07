package localtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var Now = time.Now

func SetCurrentTime(t *testing.T, value string) {
	t.Helper()

	Now = func() time.Time {
		r, err := time.Parse("2006-01-02 15:04:05", value)
		assert.NoError(t, err)

		return r
	}
}
