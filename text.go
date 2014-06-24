package bytebuffer

import (
	"bytes"
	"fmt"
	"reflect"
	"time"
)

type Writer struct {
	b *bytes.Buffer
}

func NewWriter() *Writer {
	var w Writer
	w.b = new(bytes.Buffer)
	return &w
}
func (w *Writer) Reset() {
	w.b.Reset()
}
func (w *Writer) PutAscii(text string) int {
	return w.PutText(text)
}
func (w *Writer) PutText(text string) int {
	w.b.WriteString(text)
	return len(text)
}
func (w *Writer) PutBytes(text []byte) int {
	w.b.Write(text)
	return len(text)
}
func (w *Writer) PutInt64(v int64) int {
	w.b.Write([]byte{
		byte(v >> 56),
		byte(v >> 48),
		byte(v >> 40),
		byte(v >> 32),
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	})
	return 8
}
func (w *Writer) PutInt32(v int32) int {
	w.b.Write([]byte{
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	})
	return 4
}
func (w *Writer) PutInt16(v int16) int {
	w.b.Write([]byte{
		byte(v >> 8),
		byte(v),
	})
	return 2
}
func (w *Writer) PutByte(v byte) int {
	w.b.WriteByte(v)
	return 1
}
func (w *Writer) PutTimestamp(t time.Time) int {
	milli := t.UnixNano() / 1000000
	w.PutInt64(milli)
	return 8
}
func (w *Writer) PutBool(b bool) int {
	if b {
		w.b.WriteByte(1)
	} else {
		w.b.WriteByte(0)
	}
	return 1
}
func (w *Writer) PutMap(v map[string]string) int {
	count := 2
	w.PutInt16(int16(len(v)))
	for key, value := range v {
		klen := len(key)
		vlen := len(value)
		w.PutInt16(int16(klen))
		w.PutText(key)
		w.PutInt16(int16(vlen))
		w.PutText(value)
		count += (4 + klen + vlen)
	}
	return count
}
func (w *Writer) PutFlatMap(v [][2]string) int {
	count := 2
	w.PutInt16(int16(len(v)))
	for _, value := range v {
		klen := len(value[0])
		vlen := len(value[1])
		w.PutInt16(int16(len(value[0])))
		w.PutText(value[0])
		w.PutInt16(int16(len(value[1])))
		w.PutText(value[1])
		count += (4 + klen + vlen)
	}
	return count
}
func (w *Writer) PutListString(v []string) int {
	count := 2
	w.PutInt16(int16(len(v)))
	for _, value := range v {
		w.PutInt16(int16(len(value)))
		w.PutText(value)
		count += len(value) + 2
	}
	return count
}
func (w *Writer) PutObject(v interface{}) int {
	switch v := v.(type) {
	case string:
		return w.PutText(v)
	case []byte:
		return w.PutBytes(v)
	case int32:
		return w.PutInt32(v)
	case int64:
		return w.PutInt64(v)
	case int:
		return w.PutInt32(int32(v))
	case map[string]string:
		return w.PutMap(v)
	case time.Time:
		return w.PutTimestamp(v)
	default:
		panic(fmt.Sprintf("not supported %v", reflect.TypeOf(v)))
	}
	return 0
}
func (w *Writer) Bytes() []byte {
	return w.b.Bytes()
}
