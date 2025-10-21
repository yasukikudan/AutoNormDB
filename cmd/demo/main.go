package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"

	"AutoNormDb/table/parquettable"
)

func main() {
	query := flag.String("q", "", "SQL to run")
	flag.Parse()

	const dbName = "demo"

	db := parquettable.NewDatabase(dbName)
	provider := memory.NewDBProvider(db)

	matches, err := filepath.Glob(filepath.Join("lake", "*.parquet"))
	if err != nil {
		log.Fatalf("glob parquet files: %v", err)
	}
	if len(matches) == 0 {
		log.Fatalf("no parquet files found in %s", filepath.Join("lake", "*.parquet"))
	}

	for _, path := range matches {
		tableName := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		if tableName == "" {
			log.Fatalf("could not derive table name from %s", path)
		}
		if err := parquettable.RegisterParquetTable(provider, dbName, tableName, path); err != nil {
			log.Fatalf("register parquet table %s: %v", tableName, err)
		}

		relPath := filepath.ToSlash(path)
		if err := parquettable.RegisterParquetTable(provider, dbName, relPath, path); err != nil {
			log.Fatalf("register parquet table %s: %v", relPath, err)
		}
	}

	engine := sqle.NewDefault(provider)
	ctx := sql.NewEmptyContext()

	if *query == "" {
		listTables(ctx, provider, dbName)
		return
	}

	runQuery(ctx, engine, *query)
}

func listTables(ctx *sql.Context, provider *memory.DbProvider, dbName string) {
	db, err := provider.Database(ctx, dbName)
	if err != nil {
		log.Fatalf("fetch database %s: %v", dbName, err)
	}
	names, err := db.GetTableNames(ctx)
	if err != nil {
		log.Fatalf("list tables: %v", err)
	}
	for _, name := range names {
		fmt.Println(name)
	}
}

func runQuery(ctx *sql.Context, engine *sqle.Engine, query string) {
	sch, rowIter, _, err := engine.Query(ctx, query)
	if err != nil {
		log.Fatalf("execute query: %v", err)
	}
	defer func() {
		if cerr := rowIter.Close(ctx); cerr != nil {
			log.Fatalf("close iterator: %v", cerr)
		}
	}()

	if sch != nil && len(sch) > 0 {
		headers := make([]string, len(sch))
		for i, col := range sch {
			headers[i] = col.Name
		}
		fmt.Println(strings.Join(headers, "\t"))
	}

	for {
		row, err := rowIter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("iterate rows: %v", err)
		}
		values := make([]string, len(row))
		for i, v := range row {
			values[i] = fmt.Sprint(v)
		}
		fmt.Println(strings.Join(values, "\t"))
	}

}
