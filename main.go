package main

// このプログラムはローカルの "lake" ディレクトリに存在するすべての Parquet ファイルを
// 読み込み、Arrow ベースのインメモリデータベースにロードしたうえで、go-mysql-server を
// 利用した MySQL 互換サーバーとして公開します。処理の大まかな流れは (1) Parquet ファイルの
// 探索、(2) Arrow テーブルへの変換とデータベース化、(3) SQL サーバーの起動、という 3 段階です。

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"AutoNormDb/parquetloader"
	"AutoNormDb/sqlserver"
)

var (
	dbName  = "AutoNormDB"
	address = "localhost"
	port    = 3306
)

func main() {
	// データレイクとして扱うディレクトリ名。実行時にはここから Parquet ファイルを探索します。
	dataDir := "lake"

	// ディレクトリ配下のエントリをすべて列挙し、ファイルのメタデータを取得します。
	// ReadDir は一括で結果を返すため、後続のフィルタリング処理をメモリ上でまとめて実行できます。
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	var parquetFiles []string
	for _, entry := range entries {
		// サブディレクトリはスキップし、最上位に存在するファイルのみを対象とします。
		if entry.IsDir() {
			continue
		}

		// 拡張子が .parquet かどうかを大小文字を無視して判定し、対象ファイルのパスを収集します。
		// filepath.Ext は先頭にドットを含む拡張子を返すため、strings.EqualFold を使って比較します。
		if strings.EqualFold(filepath.Ext(entry.Name()), ".parquet") {
			parquetFiles = append(parquetFiles, filepath.Join(dataDir, entry.Name()))
		}
	}

	// 一件も Parquet ファイルが見つからなかった場合は致命的エラーとして終了します。
	if len(parquetFiles) == 0 {
		log.Fatalf("no parquet files found in %s", dataDir)
	}

	// parquetloader.LoadParquetFilesIntoDB は全ファイルを読み込み、各ファイルを 1 テーブルとして
	// Arrow ベースのデータベースに登録します。戻り値の provider は go-mysql-server への接続口になります。
	provider, err := parquetloader.LoadParquetFilesIntoDB(parquetFiles, dbName)
	if err != nil {
		log.Fatal(err)
	}

	// SQL サーバーを指定アドレスで起動します。Start 内部では非同期でサーバーループが開始されます。
	if err := sqlserver.Start(provider, fmt.Sprintf("%s:%d", address, port)); err != nil {
		log.Fatal(err)
	}

	// main ゴルーチンが終了するとプロセス全体が終了してしまうため、空の select{} でブロックし続けます。
	select {}
}
