package bytebuffer

import ()

func (w *Writer) PutRecord(record ...interface{}) int {
	w.PutInt16(int16(len(record)))
	b := NewWriter()
	count := 2
	for _, value := range record {
		n := b.PutObject(value)
		w.PutInt16(int16(n))
		buf := b.Bytes()
		w.b.Write(buf)
		count += (2 + len(buf))
		b.Reset()
	}
	return count
}
