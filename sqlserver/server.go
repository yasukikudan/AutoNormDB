package sqlserver

import (
	"log"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
)

// Start launches a MySQL-compatible server backed by the provided DbProvider on the given address.
func Start(pro *memory.DbProvider, addr string) error {
	engine := sqle.NewDefault(pro)
	cfg := server.Config{
		Protocol: "tcp",
		Address:  addr,
	}
	s, err := server.NewServer(cfg, engine, sql.NewContext, memory.NewSessionBuilder(pro), nil)
	if err != nil {
		return err
	}
	go func() {
		if err := s.Start(); err != nil {
			log.Fatalf("server start: %v", err)
		}
	}()
	return nil
}
