package arrowtable

import "AutoNormDb/database"

// Database は既存の Arrow テーブル利用者との後方互換性のために公開される別名です。
// 共通のデータベース実装を再利用し、Arrow バックエンドのパーティションが他形式と
// 共存できるようにします。
type Database = database.Database

// NewDatabase は Arrow バックエンドのパーティションを他の実装と混在させられる
// データベースを構築します。
func NewDatabase(name string) *Database {
	return database.NewDatabase(name)
}
