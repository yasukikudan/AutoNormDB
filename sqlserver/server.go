package sqlserver

// このパッケージは go-mysql-server を用いて、Arrow ベースのデータベースを MySQL 互換
// プロトコルで公開するための最小限のサーバーラッパーを提供します。Start 関数でサーバーを
// 起動し、セッション生成ロジックを sessionBuilder で定義します。

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
	// go-mysql-server のエンジンを生成し、渡された DatabaseProvider を内部で利用させます。
	engine := sqle.NewDefault(pro)
	cfg := server.Config{
		Protocol: "tcp",
		Address:  addr,
	}
	// server.NewServer はリスナやコネクション管理を含むサーバーインスタンスを構築します。
	s, err := server.NewServer(cfg, engine, sql.NewContext, sessionBuilder, nil)
	if err != nil {
		return err
	}
	// s.Start() はブロッキング処理のため、ゴルーチンで起動し、エラー発生時にはプロセスごと
	// 停止させるために log.Fatalf を用いています。
	go func() {
		if err := s.Start(); err != nil {
			log.Fatalf("server start: %v", err)
		}
	}()
	return nil
}

func sessionBuilder(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
	// クライアント接続ごとにベースセッションを生成し、接続情報を go-mysql-server に提供します。
	return sql.BaseSessionFromConnection(ctx, conn, addr)
}
