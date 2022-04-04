package storage

func NewStorage(
	roTables ROTables,
	txTables TxTables,
) *storage {
	return &storage{
		roTables: roTables,
		txTables: txTables,
	}
}

type storage struct {
	roTables ROTables
	txTables TxTables
}

func (s *storage) Begin() int64 {
	return s.txTables.Begin()
}

func (s *storage) Commit(txID int64) error {
	return s.txTables.Commit(txID)
}

func (s *storage) Rollback(txID int64) error {
	return s.txTables.Rollback(txID)
}

func (s *storage) Get(key Key) (Row, error) {
	txID := s.Begin()
	defer func() {
		_ = s.Rollback(txID)
	}()
	return s.TxGet(txID, key)
}

func (s *storage) Set(key Key, row Row) error {
	txID := s.Begin()
	if err := s.TxSet(txID, key, row); err != nil {
		defer func() {
			_ = s.Rollback(txID)
		}()
		return err
	}
	return s.Commit(txID)
}

func (s *storage) TxGet(txID int64, key Key) (Row, error) {
	row, err := s.txTables.Get(txID, key)
	if err != nil || row != nil {
		return row, err
	}
	return s.roTables.Get(key)
}

func (s *storage) TxSet(txID int64, key Key, row Row) error {
	return s.txTables.Set(txID, key, row)
}
