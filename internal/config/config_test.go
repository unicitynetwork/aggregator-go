package config

import (
	"reflect"
	"testing"
)

func TestRedisSentinelEnvParsing(t *testing.T) {
	t.Setenv("REDIS_SENTINEL_ADDRS", "sentinel-1:26379, sentinel-2:26379 ,sentinel-3:26379")
	t.Setenv("REDIS_MASTER_NAME", "mymaster")
	t.Setenv("REDIS_SENTINEL_PASSWORD", "sentpass")
	t.Setenv("REDIS_SENTINEL_USERNAME", "sentuser")
	t.Setenv("REDIS_PASSWORD", "datapass")
	t.Setenv("REDIS_ROUTE_BY_LATENCY", "true")
	t.Setenv("REDIS_ROUTE_RANDOMLY", "true")

	t.Setenv("BFT_ENABLED", "false")
	t.Setenv("DISABLE_HIGH_AVAILABILITY", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	wantAddrs := []string{"sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379"}
	if !reflect.DeepEqual(cfg.Redis.SentinelAddrs, wantAddrs) {
		t.Errorf("SentinelAddrs = %v, want %v", cfg.Redis.SentinelAddrs, wantAddrs)
	}
	if cfg.Redis.MasterName != "mymaster" {
		t.Errorf("MasterName = %q, want %q", cfg.Redis.MasterName, "mymaster")
	}
	if cfg.Redis.SentinelPassword != "sentpass" {
		t.Errorf("SentinelPassword = %q, want %q", cfg.Redis.SentinelPassword, "sentpass")
	}
	if cfg.Redis.SentinelUsername != "sentuser" {
		t.Errorf("SentinelUsername = %q, want %q", cfg.Redis.SentinelUsername, "sentuser")
	}
	if cfg.Redis.Password != "datapass" {
		t.Errorf("Password = %q, want %q", cfg.Redis.Password, "datapass")
	}
	if !cfg.Redis.RouteByLatency {
		t.Errorf("RouteByLatency = false, want true")
	}
	if !cfg.Redis.RouteRandomly {
		t.Errorf("RouteRandomly = false, want true")
	}
}

func TestRedisSentinelDefaults(t *testing.T) {
	t.Setenv("BFT_ENABLED", "false")
	t.Setenv("DISABLE_HIGH_AVAILABILITY", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if len(cfg.Redis.SentinelAddrs) != 0 {
		t.Errorf("SentinelAddrs default = %v, want empty", cfg.Redis.SentinelAddrs)
	}
	if cfg.Redis.MasterName != "" {
		t.Errorf("MasterName default = %q, want empty", cfg.Redis.MasterName)
	}
	if cfg.Redis.RouteByLatency || cfg.Redis.RouteRandomly {
		t.Errorf("routing flags should default to false; got latency=%v randomly=%v",
			cfg.Redis.RouteByLatency, cfg.Redis.RouteRandomly)
	}
}

func TestGetEnvStringSliceOrDefault(t *testing.T) {
	t.Run("unset returns default", func(t *testing.T) {
		def := []string{"a", "b"}
		got := getEnvStringSliceOrDefault("UNICITY_TEST_UNSET_VAR", def)
		if !reflect.DeepEqual(got, def) {
			t.Errorf("got %v, want %v", got, def)
		}
	})

	t.Run("trims and splits", func(t *testing.T) {
		t.Setenv("UNICITY_TEST_SLICE", " a , b,c ,, d ")
		got := getEnvStringSliceOrDefault("UNICITY_TEST_SLICE", nil)
		want := []string{"a", "b", "c", "d"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("only commas returns default", func(t *testing.T) {
		t.Setenv("UNICITY_TEST_SLICE_EMPTY", " , , ")
		def := []string{"x"}
		got := getEnvStringSliceOrDefault("UNICITY_TEST_SLICE_EMPTY", def)
		if !reflect.DeepEqual(got, def) {
			t.Errorf("got %v, want %v", got, def)
		}
	})
}
