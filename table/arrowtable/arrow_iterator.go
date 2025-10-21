package arrowtable

// このファイルでは Arrow のレコードバッチを go-mysql-server の行イテレータに変換する
// ヘルパーを提供します。ArrowRowIter は engine/arrowbackend に存在し、ここでは互換性の
// ために単一レコードをそのイテレータへアダプトする機能のみ保持しています。

import (
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/dolthub/go-mysql-server/sql"

	"AutoNormDb/engine/arrowbackend"
)

type singleRecordSource struct {
	rec     arrow.Record
	yielded bool
}

// NewArrowRowIterFromRecord wraps a single Arrow record and exposes it as a sql.RowIter.
func NewArrowRowIterFromRecord(rec arrow.Record) sql.RowIter {
	if rec != nil {
		rec.Retain()
	}
	src := &singleRecordSource{rec: rec}
	return arrowbackend.NewArrowRowIter(src, nil, nil)
}

func (s *singleRecordSource) Next() bool {
	if s.yielded || s.rec == nil {
		return false
	}
	s.yielded = true
	return true
}

func (s *singleRecordSource) Record() arrow.Record { return s.rec }

func (s *singleRecordSource) Err() error { return nil }

func (s *singleRecordSource) Release() {
	if s.rec != nil {
		s.rec.Release()
		s.rec = nil
	}
}
