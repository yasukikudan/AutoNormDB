package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"

	"AutoNormDb/internal/arrowfile"
)

func main() {
	query := flag.String("q", "", "SQL to run against file-backed tables")
	rootsFlag := flag.String("roots", ".", "comma-separated list of allowed filesystem roots")
	enableGlob := flag.Bool("glob", true, "whether to expand wildcard patterns")
	flag.Parse()

	if *query == "" {
		log.Fatal("provide a SQL query with -q")
	}

	cfg := arrowfile.ProviderConfig{
		DatabaseName: "AutoNormDB",
		AllowedRoots: splitRoots(*rootsFlag),
		EnableGlob:   *enableGlob,
		CacheEntries: 8,
		CacheTTL:     time.Minute,
	}

	provider := arrowfile.NewProvider(cfg)
	engine := sqle.NewDefault(provider)
	ctx := sql.NewEmptyContext()

	sch, iter, _, err := engine.Query(ctx, *query)
	if err != nil {
		log.Fatalf("execute query: %v", err)
	}
	defer func() {
		if cerr := iter.Close(ctx); cerr != nil {
			log.Fatalf("close iterator: %v", cerr)
		}
	}()

	if len(sch) > 0 {
		for i, col := range sch {
			if i > 0 {
				fmt.Print("\t")
			}
			fmt.Print(col.Name)
		}
		fmt.Println()
	}

	for {
		row, err := iter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("iterate rows: %v", err)
		}
		for i, v := range row {
			if i > 0 {
				fmt.Print("\t")
			}
			fmt.Print(v)
		}
		fmt.Println()
	}
}

func splitRoots(input string) []string {
	var roots []string
	for _, part := range strings.Split(input, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			roots = append(roots, trimmed)
		}
	}
	if len(roots) == 0 {
		roots = append(roots, ".")
	}
	return roots
}
