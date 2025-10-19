package main

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
	dataDir := "lake"

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	var parquetFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if strings.EqualFold(filepath.Ext(entry.Name()), ".parquet") {
			parquetFiles = append(parquetFiles, filepath.Join(dataDir, entry.Name()))
		}
	}

	if len(parquetFiles) == 0 {
		log.Fatalf("no parquet files found in %s", dataDir)
	}

	provider, err := parquetloader.LoadParquetFilesIntoDB(parquetFiles, dbName)
	if err != nil {
		log.Fatal(err)
	}

	if err := sqlserver.Start(provider, fmt.Sprintf("%s:%d", address, port)); err != nil {
		log.Fatal(err)
	}

	select {}
}
