package main

// このプログラムはローカルの "lake" ディレクトリをデータレイクとして扱い、
// FROM 句に指定されたファイルパスを動的に Arrow ベースのテーブルとして解決します。
// go-mysql-server を利用した MySQL 互換サーバーを起動し、CLI と同じファイル解決
// ロジックを共有することで、CUI と SQL クライアントのどちらからも一貫した操作性を提供します。

import (
	"fmt"
	"log"
	"time"

	"AutoNormDb/internal/arrowfile"
	"AutoNormDb/sqlserver"
)

var (
	dbName  = "AutoNormDB"
	address = "localhost"
	port    = 3306
)

func main() {
	cfg := arrowfile.ProviderConfig{
		DatabaseName: dbName,
		AllowedRoots: []string{"lake"},
		EnableGlob:   true,
		CacheEntries: 8,
		CacheTTL:     time.Minute,
	}

	provider := arrowfile.NewProvider(cfg)

	// SQL サーバーを指定アドレスで起動します。Start 内部では非同期でサーバーループが開始されます。
	if err := sqlserver.Start(provider, fmt.Sprintf("%s:%d", address, port)); err != nil {
		log.Fatal(err)
	}

	// main ゴルーチンが終了するとプロセス全体が終了してしまうため、空の select{} でブロックし続けます。
	select {}
}
