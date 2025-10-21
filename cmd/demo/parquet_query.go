package main

import (
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var parquetRefPattern = regexp.MustCompile(`(?i)(from|join)\s+([^\s,()]+\.parquet)`) // matches FROM/JOIN references to Parquet files

// rewriteParquetReferences detects Parquet file references in FROM/JOIN clauses,
// registers the encountered file paths, and ensures they are backtick quoted so
// that go-mysql-server's parser can handle special characters such as forward
// slashes.
func rewriteParquetReferences(query string) (string, []string) {
	matches := parquetRefPattern.FindAllStringSubmatchIndex(query, -1)
	if len(matches) == 0 {
		return query, nil
	}

	refs := make(map[string]struct{}, len(matches))
	var rewritten strings.Builder
	last := 0

	for _, idx := range matches {
		tokenStart := idx[4]
		tokenEnd := idx[5]
		rewritten.WriteString(query[last:tokenStart])

		token := query[tokenStart:tokenEnd]
		cleaned := strings.Trim(token, "`\"")
		cleaned = strings.TrimSpace(cleaned)
		if cleaned == "" {
			rewritten.WriteString(token)
			last = tokenEnd
			continue
		}

		refs[cleaned] = struct{}{}
		rewritten.WriteByte('`')
		rewritten.WriteString(cleaned)
		rewritten.WriteByte('`')

		last = tokenEnd
	}

	rewritten.WriteString(query[last:])

	list := make([]string, 0, len(refs))
	for ref := range refs {
		list = append(list, ref)
	}
	sort.Strings(list)

	return rewritten.String(), list
}

// normalizeParquetPath converts a SQL reference to a filesystem path by
// translating directory separators to the current platform's representation and
// collapsing any redundant elements.
func normalizeParquetPath(ref string) string {
	cleaned := strings.TrimSpace(ref)
	if cleaned == "" {
		return cleaned
	}

	cleaned = strings.ReplaceAll(cleaned, "\\", "/")
	return filepath.Clean(filepath.FromSlash(cleaned))
}
