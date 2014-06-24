package bytebuffer

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"testing"
	"time"
)

var h = func(t string) string {
	return hex.EncodeToString([]byte(t))
}

func TestText(t *testing.T) {
	table := [][2]interface{}{
		{"hello", h("hello")},
		{[]byte("hello"), h("hello")},
		{int32(0x04030201), "04030201"},
		{map[string]string{"name": "bob"}, "00010004" + h("name") + "0003" + h("bob")},
	}
	for _, vals := range table {
		w := NewWriter()
		w.PutObject(vals[0])
		if hex.EncodeToString(w.Bytes()) != vals[1] {
			t.Fatalf("encoding fail : %v %v %v", vals[0], vals[1], hex.EncodeToString(w.Bytes()))
		}
	}
}

type Record struct {
	Mid        int
	Uid        string
	Ts         time.Time
	Type       string
	ModEventId string
	Atype      string
	Data       map[string]string
	Data2      [][2]string
	TsHour     time.Time
}

func (r *Record) Cass() []byte {
	//	var w Writer
	w := NewWriter()
	w.PutInt32(int32(r.Mid))
	w.PutText(r.Uid)
	w.PutTimestamp(r.Ts)
	w.PutText(r.Type)
	w.PutText(r.ModEventId)
	w.PutText(r.Atype)
	//	w.PutMap(r.Data)
	w.PutFlatMap(r.Data2)
	w.PutTimestamp(r.TsHour)
	return w.Bytes()
}
func (r *Record) CassObject() []byte {
	var w Writer
	params := []interface{}{
		int32(r.Mid),
		r.Uid,
		r.Ts,
		r.Type,
		r.ModEventId,
		r.Atype,
		r.Data,
		r.TsHour,
	}
	for _, param := range params {
		w.PutObject(param)
	}
	return w.Bytes()
}
func (r *Record) Json() ([]byte, error) {
	return json.Marshal(r)
}
func (r *Record) CassStream(w *Writer) {
	params := []interface{}{
		int32(r.Mid),
		r.Uid,
		r.Ts,
		r.Type,
		r.ModEventId,
		r.Atype,
		r.Data,
		r.TsHour,
	}
	w.PutRecord(params...)
}
func (r *Record) Csv() []string {
	d, _ := json.Marshal(r.Data)

	return []string{
		strconv.Itoa(r.Mid),
		r.Uid,
		strconv.Itoa(int(r.Ts.UnixNano() / 1000000)),
		r.Type,
		r.ModEventId,
		r.Atype,
		string(d),
		strconv.Itoa(int(r.TsHour.UnixNano() / 1000000)),
	}
}

var record *Record

func init() {
	record = &Record{
		Mid:        400,
		Uid:        "12345678901234567890123456789012",
		Ts:         time.Now(),
		Type:       "vis",
		ModEventId: "",
		Atype:      "",
		Data:       map[string]string{"adid": "2345"},
		TsHour:     time.Now(),
		Data2:      [][2]string{{"adid", "2345"}},
	}
}
func BenchmarkCass(b *testing.B) {
	for i := 0; i < b.N; i++ {
		record.Cass()
	}
}
func BenchmarkCassStream(b *testing.B) {
	w := NewWriter()
	for i := 0; i < b.N; i++ {
		record.CassStream(w)
		w.Reset()
	}
}

// func BenchmarkCassObject(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		record.CassObject()
// 	}
// }
// func BenchmarkJSON(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		record.Json()
// 	}
// }
// func BenchmarkCSV(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		record.Csv()
// 	}
// }
