package dyndb

import (
	"github.com/viant/dsc"
)

func register() {
	dsc.RegisterManagerFactory("dyndb", newManagerFactory())
	dsc.RegisterDatastoreDialect("dyndb", newDialect())
}

func init() {
	register()
}
