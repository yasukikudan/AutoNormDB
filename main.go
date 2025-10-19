package main

import (
	"fmt"
	"log"

	"AutoNormDb/parquetloader"
	"AutoNormDb/sqlserver"
)

var (
	dbName    = "AutoNormDB"
	tableName = "mytable"
	address   = "localhost"
	port      = 3306
)

func main() {
	provider, err := parquetloader.LoadParquetIntoDB("data/dummy_web_logs.parquet", dbName, tableName)
	if err != nil {
		log.Fatal(err)
	}

	if err := sqlserver.Start(provider, fmt.Sprintf("%s:%d", address, port)); err != nil {
		log.Fatal(err)
	}

	select {}
}
