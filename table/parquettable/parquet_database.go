package parquettable

import "AutoNormDb/database"

// Database は共通実装を再公開する別名であり、Parquet バックエンドのパーティションが
// 他の実装と同じプロバイダー内で扱えるようにします。
type Database = database.Database

// NewDatabase は Parquet バックエンドのパーティションを格納できるデータベースを構築します。
func NewDatabase(name string) *Database {
	return database.NewDatabase(name)
}
