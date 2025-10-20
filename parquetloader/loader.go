package parquetloader

// このパッケージは Apache Arrow と go-mysql-server を連携させ、Parquet ファイルを
// メモリ上の Arrow テーブルに読み込んだ後、SQL サーバーから参照できるようにする
// ローダー処理を提供します。Parquet ファイルの構造を保持したまま、SQL で参照できる
// テーブルとして登録することが主な役割です。

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/apache/arrow/go/v15/arrow"
	arrow_array "github.com/apache/arrow/go/v15/arrow/array"
	arrow_memory "github.com/apache/arrow/go/v15/arrow/memory"
	arrow_file "github.com/apache/arrow/go/v15/parquet/file"
	arrow_pqarrow "github.com/apache/arrow/go/v15/parquet/pqarrow"

	"AutoNormDb/arrowtable"
)

// LoadParquetIntoDB loads the given parquet file into an Arrow-backed
// go-mysql-server database, creating the database and table as needed. It
// returns a provider ready to be served via go-mysql-server.
func LoadParquetIntoDB(filePath, dbName, tableName string) (*arrowtable.Provider, error) {
	// 読み込み先となるデータベースと、SQL エンジンに公開するためのプロバイダを生成します。
	db := arrowtable.NewDatabase(dbName)
	pro := arrowtable.NewProvider(db)

	// 単一ファイルのロード処理を内部ヘルパーに委譲します。context.Background を用いて
	// キャンセルされない読み込みコンテキストを生成しています。
	if err := loadParquetFileIntoDB(context.Background(), filePath, tableName, db); err != nil {
		return nil, err
	}

	return pro, nil
}

// LoadParquetFilesIntoDB loads multiple parquet files into the same database.
// Each file will become a table named after the file's base name without the
// extension.
func LoadParquetFilesIntoDB(filePaths []string, dbName string) (*arrowtable.Provider, error) {
	// 入力検証：空スライスで呼ばれた場合は処理不能なのでエラーを返します。
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("no parquet files provided")
	}

	// 共通の Arrow データベースと SQL プロバイダを準備します。
	db := arrowtable.NewDatabase(dbName)
	pro := arrowtable.NewProvider(db)

	// すべてのファイル読み込みで共有するコンテキストを生成します。
	ctx := context.Background()
	for _, filePath := range filePaths {
		// ファイル名から拡張子を除いた文字列をテーブル名として利用します。
		tableName := strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))
		if tableName == "" {
			return nil, fmt.Errorf("could not derive table name from %q", filePath)
		}

		// 各 Parquet ファイルを Arrow テーブルとして読み込み、データベースに登録します。
		if err := loadParquetFileIntoDB(ctx, filePath, tableName, db); err != nil {
			return nil, err
		}
	}

	return pro, nil
}

func loadParquetFileIntoDB(ctx context.Context, filePath, tableName string, db *arrowtable.Database) error {
	// Parquet ファイルを読み取り専用でオープンします。2 番目の引数 false はメモリマップを無効化し、
	// 通常のファイルアクセスを行う設定です。
	f, err := arrow_file.OpenParquetFile(filePath, false)
	if err != nil {
		return fmt.Errorf("open parquet %q: %w", filePath, err)
	}
	defer f.Close()

	// Arrow アロケータを準備し、4096 行単位でレコードバッチを取り出す設定を行います。
	pool := arrow_memory.NewGoAllocator()
	props := arrow_pqarrow.ArrowReadProperties{BatchSize: 4096}
	fr, err := arrow_pqarrow.NewFileReader(f, props, pool)
	if err != nil {
		return fmt.Errorf("new pqarrow reader for %q: %w", filePath, err)
	}

	// Parquet 内のすべてのカラムを対象に Arrow RecordReader を生成します。
	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("get record reader for %q: %w", filePath, err)
	}
	defer rr.Release()

	schema := rr.Schema()
	var records []arrow.Record
	totalRows := int64(0)
	batches := 0

	for rr.Next() {
		rec := rr.Record()
		if rec == nil {
			continue
		}

		// rr.Record が返すレコードは次の Next 呼び出しで再利用されるため、Retain で参照カウントを
		// 増やし所有権を確保します。最後にまとめて Release してメモリリークを防ぎます。
		rec.Retain()
		records = append(records, rec)
		totalRows += int64(rec.NumRows())
		batches++
	}

	// 収集した RecordBatch をまとめて Arrow Table に組み立てます。
	arrTable := arrow_array.NewTableFromRecords(schema, records)
	for _, rec := range records {
		rec.Release()
	}
	defer arrTable.Release()

	// ArrowBackedTable にラップすることで go-mysql-server に適合するテーブル表現に変換します。
	arrowTbl, err := arrowtable.NewArrowBackedTable(tableName, arrTable)
	if err != nil {
		return fmt.Errorf("wrap arrow table for %q: %w", filePath, err)
	}

	// Arrow テーブルをデータベースに登録し、重複時にはエラーを返します。
	if err := db.AddTable(arrowTbl); err != nil {
		return fmt.Errorf("register table %s: %w", tableName, err)
	}

	// 処理したレコードバッチ数と行数をログに出力して、読み込み状況を確認できるようにします。
	log.Printf("batches=%d rows=%d for table %s from %s", batches, totalRows, tableName, filePath)
	return nil
}
