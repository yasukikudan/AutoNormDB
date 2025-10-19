// Copyright 2020-2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	arrow "github.com/apache/arrow/go/v15/arrow"
	arrow_array "github.com/apache/arrow/go/v15/arrow/array"
	arrow_memory "github.com/apache/arrow/go/v15/arrow/memory"
	arrow_file "github.com/apache/arrow/go/v15/parquet/file"
	arrow_pqarrow "github.com/apache/arrow/go/v15/parquet/pqarrow"

	"github.com/dolthub/vitess/go/vt/proto/query"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// この例ではParquetファイルを読み込み、メモリ上のDBに投入し、MySQL互換サーバとしてクエリ可能にします。
// 起動後、以下のようにMySQLクライアントからParquetデータに対してSQLクエリを実行できます:
// Parquetファイルの内容をSQLで簡単に検索・集計できます。MySQL互換クライアントで利用可能です。

var (
	dbName    = "AutoNormDB"
	tableName = "mytable"
	address   = "localhost"
	port      = 3306
)

// For go-mysql-server developers: Remember to update the snippet in the README when this file changes.

func main() {

	pro, err := LoadParquetIntoDB("dummy_web_logs.parquet", dbName, tableName)
	if err != nil {
		log.Fatal(err)
	}
	// サーバ起動（任意）
	if err := startServerExample(pro, fmt.Sprintf("%s:%d", address, port)); err != nil {
		log.Fatal(err)
	}
	select {}
}

// Parquetを読み込み、Arrow経由で go-mysql-server のメモリDBへ投入。
// dbName/tableName は作成され、Parquetスキーマから自動でSQLスキーマが生成されます。
func LoadParquetIntoDB(filePath, dbName, tableName string) (*memory.DbProvider, error) {
	// 1) Parquet -> Arrow
	f, err := arrow_file.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}
	defer f.Close()

	ctx := context.Background()
	pool := arrow_memory.NewGoAllocator()
	props := arrow_pqarrow.ArrowReadProperties{
		BatchSize: 4096, // 例：4K
	}
	fr, err := arrow_pqarrow.NewFileReader(f, props, pool)
	if err != nil {
		return nil, fmt.Errorf("new pqarrow reader: %w", err)
	}

	rr, err := fr.GetRecordReader(ctx, nil, nil) // 全RowGroup/全列
	if err != nil {
		return nil, fmt.Errorf("get record reader: %w", err)
	}
	defer rr.Release()

	// 2) Arrow Schema -> SQL Schema
	arSchema := rr.Schema()
	sqlSchema, err := makeSQLSchemaFromArrow(arSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("make sql schema: %w", err)
	}

	// 3) DB とテーブル作成（★ PrimaryKeySchema を渡す必要あり）
	db := memory.NewDatabase(dbName)
	db.BaseDatabase.EnablePrimaryKeyIndexes()
	pro := memory.NewDBProvider(db)

	pkSchema := sql.NewPrimaryKeySchema(sqlSchema)
	tbl := memory.NewTable(db, tableName, pkSchema, db.GetForeignKeyCollection())
	db.AddTable(tableName, tbl)

	// 4) 行を逐次 Insert
	sess := memory.NewSession(sql.NewBaseSession(), pro)
	qctx := sql.NewContext(ctx, sql.WithSession(sess))

	inserted := 0
	batch := 0
	for rr.Next() {
		rec := rr.Record()
		if rec == nil {
			continue
		}
		rec.Retain() // ★ 受け取ったら保持
		nRows := int(rec.NumRows())
		nCols := int(rec.NumCols())

		if nRows == 0 || nCols == 0 { // ★ 空バッチ/空列はスキップ
			rec.Release()
			batch++
			continue
		}

		for r := 0; r < nRows; r++ {
			rowVals := make([]interface{}, nCols)
			for c := 0; c < nCols; c++ {
				col := rec.Column(c)
				val, convErr := valueAt(col, arSchema.Field(c), r)
				if convErr != nil {
					rec.Release()
					return nil, fmt.Errorf("convert value col=%d row=%d: %w", c, r, convErr)
				}
				rowVals[c] = val
			}
			if err := tbl.Insert(qctx, sql.NewRow(rowVals...)); err != nil {
				rec.Release()
				return nil, fmt.Errorf("insert row %d: %w", r, err)
			}
			inserted++
		}

		rec.Release() // ★ 使い終わったら必ず解放
		batch++
	}
	// （必要なら）ここで rr.Err() を確認できる実装もある
	log.Printf("batches=%d inserted=%d", batch, inserted)

	log.Printf("Imported %d rows into %s.%s", inserted, dbName, tableName)
	return pro, nil
}

// Arrow Schema -> go-mysql-server Schema
func makeSQLSchemaFromArrow(s *arrow.Schema, tableName string) (sql.Schema, error) {
	out := make(sql.Schema, len(s.Fields()))
	for i, f := range s.Fields() {
		tp, err := arrowTypeToSQLType(f.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}
		out[i] = &sql.Column{
			Name:     f.Name,
			Type:     tp,
			Nullable: f.Nullable,
			Source:   tableName,
			// 必要なら PrimaryKey: true を付与
		}
	}
	return out, nil
}

// 最低限の型マッピング（拡張しやすく分離）
func arrowTypeToSQLType(dt arrow.DataType) (sql.Type, error) {
	switch t := dt.(type) {
	case *arrow.StringType, *arrow.LargeStringType:
		return types.Text, nil
	case *arrow.BooleanType:
		return types.Boolean, nil
	case *arrow.Int32Type:
		return types.Int32, nil
	case *arrow.Int64Type:
		return types.Int64, nil
	case *arrow.Float64Type:
		return types.Float64, nil
	case *arrow.TimestampType:
		return types.MustCreateDatetimeType(query.Type_DATETIME, 6), nil
	default:
		return nil, fmt.Errorf("unsupported arrow type: %s", t)
	}
}

// 1セル取り出し（Arrow Array -> Go値）
// List/Struct/Map は未対応。必要なら JSON 化ハンドラを追加してください。
func valueAt(col arrow.Array, field arrow.Field, row int) (interface{}, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	switch arr := col.(type) {
	case *arrow_array.String:
		return arr.Value(row), nil
	case *arrow_array.LargeString:
		return arr.Value(row), nil
	case *arrow_array.Boolean:
		return arr.Value(row), nil
	case *arrow_array.Int32:
		return arr.Value(row), nil
	case *arrow_array.Int64:
		return arr.Value(row), nil
	case *arrow_array.Float64:
		return arr.Value(row), nil
	case *arrow_array.Timestamp:
		tsType := field.Type.(*arrow.TimestampType)
		iv := arr.Value(row)
		return arrowTimestampToTime(int64(iv), tsType), nil
	default:
		return nil, fmt.Errorf("unsupported array kind: %T", col)
	}
}

// Arrow Timestamp -> time.Time（UTC）
func arrowTimestampToTime(v int64, t *arrow.TimestampType) time.Time {
	switch t.Unit {
	case arrow.Second:
		return time.Unix(v, 0).UTC()
	case arrow.Millisecond:
		sec := v / 1_000
		nsec := (v % 1_000) * int64(time.Millisecond)
		return time.Unix(sec, nsec).UTC()
	case arrow.Microsecond:
		sec := v / 1_000_000
		nsec := (v % 1_000_000) * int64(time.Microsecond)
		return time.Unix(sec, nsec).UTC()
	case arrow.Nanosecond:
		sec := v / 1_000_000_000
		nsec := v % 1_000_000_000
		return time.Unix(sec, nsec).UTC()
	default:
		return time.Unix(v, 0).UTC()
	}
}

// ---- おまけ：エンジン起動例（任意） ----
// 取り込んだ DB を MySQL互換サーバとして開く場合
func startServerExample(pro *memory.DbProvider, addr string) error {
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
