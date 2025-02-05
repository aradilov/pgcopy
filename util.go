package pgcopy

import (
	"encoding/binary"
	"strings"
)

func AppendUint16(buf []byte, n uint16) []byte {
	wp := len(buf)
	buf = append(buf, 0, 0)
	binary.BigEndian.PutUint16(buf[wp:], n)
	return buf
}

func AppendUint32(buf []byte, n uint32) []byte {
	wp := len(buf)
	buf = append(buf, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(buf[wp:], n)
	return buf
}

func AppendUint64(buf []byte, n uint64) []byte {
	wp := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[wp:], n)
	return buf
}

func AppendInt16(buf []byte, n int16) []byte {
	return AppendUint16(buf, uint16(n))
}

func AppendInt32(buf []byte, n int32) []byte {
	return AppendUint32(buf, uint32(n))
}

func AppendInt64(buf []byte, n int64) []byte {
	return AppendUint64(buf, uint64(n))
}

func SetInt32(buf []byte, n int32) {
	binary.BigEndian.PutUint32(buf, uint32(n))
}

func sanitize(ident []string, sep string) string {
	parts := make([]string, len(ident))
	for i := range ident {
		s := strings.ReplaceAll(ident[i], string([]byte{0}), "")
		parts[i] = `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return strings.Join(parts, sep)
}
