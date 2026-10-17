package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLongPollConfigValidation(t *testing.T) {
	base := func() *Config {
		c := &Config{}
		c.Server = ServerConfig{WriteTimeout: 30 * time.Second, LongPollMaxWait: 20 * time.Second, LongPollMaxWaiters: 1024}
		return c
	}
	require.NoError(t, base().validateLongPoll(), "the defaults fit: 20s + 8s reserve < 30s")

	c := base()
	c.Server.LongPollMaxWait = 22 * time.Second
	require.Error(t, c.validateLongPoll(), "22s + 8s is not below the 30s method budget")

	c = base()
	c.Server.WriteTimeout = 25 * time.Second
	require.Error(t, c.validateLongPoll(), "max wait + reserve must stay below a finite WRITE_TIMEOUT")

	c = base()
	c.Server.WriteTimeout = 0 // unlimited transport timeout: the application deadline still bounds
	require.NoError(t, c.validateLongPoll())

	c = base()
	c.Server.LongPollMaxWaiters = 0 // parking disabled: supported, nothing to fit
	c.Server.LongPollMaxWait = time.Hour
	require.NoError(t, c.validateLongPoll())

	c = base()
	c.Server.LongPollMaxWaiters = -1
	require.Error(t, c.validateLongPoll())
	c = base()
	c.Server.LongPollMaxWait = -time.Second
	require.Error(t, c.validateLongPoll())
}

func TestLongPollEnvDefaults(t *testing.T) {
	t.Setenv("LONG_POLL_MAX_WAIT", "")
	t.Setenv("LONG_POLL_MAX_WAITERS", "")
	c, err := Load()
	if err != nil {
		t.Skipf("Load needs more environment: %v", err)
	}
	require.Equal(t, 20*time.Second, c.Server.LongPollMaxWait)
	require.Equal(t, 1024, c.Server.LongPollMaxWaiters)
	t.Setenv("LONG_POLL_MAX_WAITERS", "0")
	c, err = Load()
	require.NoError(t, err)
	require.Equal(t, 0, c.Server.LongPollMaxWaiters)
}
