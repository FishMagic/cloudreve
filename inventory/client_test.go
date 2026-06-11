package inventory

import (
	"testing"

	"github.com/cloudreve/Cloudreve/v4/pkg/conf"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/stretchr/testify/assert"
)

type mockConfigProvider struct {
	db conf.Database
}

func (m *mockConfigProvider) Database() *conf.Database {
	return &m.db
}

func (m *mockConfigProvider) System() *conf.System {
	return &conf.System{Debug: false}
}

func (m *mockConfigProvider) SSL() *conf.SSL {
	return nil
}

func (m *mockConfigProvider) Unix() *conf.Unix {
	return nil
}

func (m *mockConfigProvider) Slave() *conf.Slave {
	return nil
}

func (m *mockConfigProvider) Redis() *conf.Redis {
	return nil
}

func (m *mockConfigProvider) Cors() *conf.Cors {
	return nil
}

func (m *mockConfigProvider) OptionOverwrite() map[string]any {
	return nil
}

func TestNewRawEntClient_CloudflareD1(t *testing.T) {
	l := logging.Default()
	cfg := &mockConfigProvider{
		db: conf.Database{
			Type:     conf.CloudflareD1,
			User:     "test-account-id",
			Password: "test-api-token",
			Name:     "test-database-uuid",
		},
	}

	client, err := NewRawEntClient(l, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()
}

func TestNewRawEntClient_CloudflareD1_ConnectionString(t *testing.T) {
	l := logging.Default()
	cfg := &mockConfigProvider{
		db: conf.Database{
			Type:        conf.CloudflareD1,
			DatabaseURL: "d1://test-account-id:test-api-token@test-database-uuid",
		},
	}

	client, err := NewRawEntClient(l, cfg)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()
}
