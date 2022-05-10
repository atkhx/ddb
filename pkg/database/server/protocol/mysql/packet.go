package mysql

import (
	"net"
	"strings"
)

type Packet struct {
	data []byte
}

func (p *Packet) appendBytes(b ...byte) {
	p.data = append(p.data, b...)
}

func (p *Packet) appendStringNulByte(s string) {
	p.appendBytes([]byte(s)...)
	p.appendBytes(0)
}

func (p *Packet) appendStringLengthEncoded(value string) {
	p.appendIntegerLengthEncoded(uint64(len(value)))
	p.appendBytes([]byte(value)...)
}

func (p *Packet) appendIntegerLengthEncoded(n uint64) {
	switch {
	case n <= 250:
		p.appendBytes(byte(n))
		return

	case n <= 0xffff:
		p.appendBytes(0xfc, byte(n), byte(n>>8))
		return

	case n <= 0xffffff:
		p.appendBytes(0xfd, byte(n), byte(n>>8), byte(n>>16))
		return

	default:
		p.appendBytes(0xfe, byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
			byte(n>>32), byte(n>>40), byte(n>>48), byte(n>>56))
	}
}

func (p *Packet) appendIntTwoBytes(i int) {
	p.appendBytes(byte(i), byte(i>>8))
}

func (p *Packet) calculateLength() {
	length := len(p.data) - 4
	p.data[0] = byte(length)
	p.data[1] = byte(length >> 8)
	p.data[2] = byte(length >> 16)
}

func (p *Packet) readLength() int {
	return p.read3BytesInt(0)
}

func (p *Packet) read3BytesInt(pos int) int {
	return int(uint32(p.data[pos]) | uint32(p.data[pos+1])<<8 | uint32(p.data[pos+2])<<16)
}

func (p *Packet) read4BytesInt(pos int) int {
	return int(uint32(p.data[pos]) | uint32(p.data[pos+1])<<8 | uint32(p.data[pos+2])<<16 | uint32(p.data[pos+3])<<24)
}

func (p *Packet) readStringNulByte(pos int) (s string, n int) {

	for n = pos; p.data[n] != 0; n++ {
	}

	return string(p.data[pos:n]), n
}

func (p *Packet) readBytes(conn net.Conn) (err error) {
	data := make([]byte, 1024)
	size := 0

	if size, err = conn.Read(data); err != nil {
		return err
	}

	p.data = make([]byte, size)
	copy(p.data, data)

	return nil
}

func (p *Packet) readSequence() byte {
	return p.data[3]
}

func (p *Packet) readQuery() string {
	length := p.readLength()
	return strings.Trim(string(p.data[5:4+length]), " ")
}
