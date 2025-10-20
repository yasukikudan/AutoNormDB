package sqlserver

import (
	"context"
	"log"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

// Start launches a MySQL-compatible server backed by the provided DatabaseProvider on the given address.
func Start(pro sql.DatabaseProvider, addr string) error {
	engine := sqle.NewDefault(pro)
	cfg := server.Config{
		Protocol: "tcp",
		Address:  addr,
	}
	s, err := server.NewServer(cfg, engine, sql.NewContext, sessionBuilder, nil)
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

func sessionBuilder(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
	return sql.BaseSessionFromConnection(ctx, conn, addr)
}
