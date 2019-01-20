package dyndb_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"testing"
)

func TestNewConnection(t *testing.T) {
	config, err := dsc.NewConfigWithParameters("dyndbc", "", "", nil)
	if !assert.Nil(t, err) {
		return
	}
	factory := dsc.NewManagerFactory()
	manager, err := factory.Create(config)
	if !assert.Nil(t, err) {
		return
	}
	provider := manager.ConnectionProvider()
	_, err = provider.NewConnection()
	assert.Nil(t, err)
}
