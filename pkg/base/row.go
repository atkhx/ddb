package base

type Row struct {
	PKey  Key
	Data  interface{}
	CTime int64
	DTime int64
}
