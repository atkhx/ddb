package mysql

import (
	"fmt"
	"net"

	"github.com/atkhx/ddb/pkg/database"
)

func NewConnection(conn net.Conn) *connection {
	return &connection{conn: conn}
}

type connection struct {
	conn net.Conn

	authPacket authPacket
	sequence   byte
}

type response struct {
	status        statusFlag
	affectedRows  int64
	lastInsertId  int64
	warningsCount int64
}

func (c *connection) InitConnection() error {
	if err := c.writeInitialPacket(); err != nil {
		return err
	}

	p, err := c.readAuthPacket()
	if err != nil {
		return err
	}
	c.authPacket = p

	c.nextSequence()
	return c.writeOkPacket(response{}, iOK)
}

func (c *connection) ReadRequest() (database.DbRequest, error) {
	c.resetSequence()

	p, err := c.readPacket()
	if err != nil {
		return nil, err
	}
	return p.readQuery(), nil
}

func (c *connection) WriteResult(result database.DbResult) error {
	switch {
	case result.Error != nil:
		return c.writeError(*result.Error)
	case result.FetchResult != nil:
		return c.writeFetchResult(*result.FetchResult)
	case result.ExecResult != nil:
		return c.writeExecResult(*result.ExecResult)
	}
	return c.writeOkPacket(response{}, iOK)
}

func (c *connection) readPacket() (Packet, error) {
	pak := Packet{}
	err := pak.readBytes(c.conn)

	if err == nil {
		fmt.Println(pak.readQuery())
	}

	return pak, err
}

func (c *connection) resetSequence() byte {
	c.sequence = 0
	return c.sequence
}

func (c *connection) nextSequence() byte {
	c.sequence++
	return c.sequence
}

func (c *connection) writeInitialPacket() error {
	p := Packet{}
	p.appendBytes(0, 0, 0, c.resetSequence())
	p.appendBytes(protocolVersion)
	p.appendStringNulByte("DDB ver 0.1")

	// @todo узнать формат
	// thread id OR connection id
	p.appendBytes(10, 0, 0, 0)

	// first 8 bytes of the plugin provided data (scramble)
	p.appendStringNulByte("12345678")

	// server capabilities (two lower bytes)
	p.appendIntTwoBytes(int(clientProtocol41))
	//p.appendBytes(255, 255)

	// @todo узнать список возможных
	// server character set
	p.appendBytes(33)

	// @todo генерировать из констант
	// server status
	p.appendBytes(2, 0)

	// @todo генерировать из констант
	// server1 capabilities (two upper bytes)
	p.appendBytes(255, 193)

	// length of the scramble
	p.appendBytes(21)

	// reserved, always 0
	p.appendBytes(make([]byte, 10)...)

	// rest of the plugin provided data (at least 12 bytes)
	p.appendStringNulByte("123456789012")
	p.appendStringNulByte("mysql_native_password")

	p.calculateLength()

	_, err := c.conn.Write(p.data)
	return err
}

func (c *connection) readAuthPacket() (authPacket, error) {
	p, err := c.readPacket()
	if err != nil {
		return authPacket{}, err
	}

	ap := authPacket{
		length:          p.readLength(),
		flags:           clientFlag(p.read4BytesInt(4)),
		maxPacketLength: p.read4BytesInt(8),

		collation: p.data[12],
	}

	pos := 13
	ap.filter = make([]byte, 23)
	copy(ap.filter, p.data[pos:pos+23])

	pos = 13 + 23
	ap.user, pos = p.readStringNulByte(pos)
	// some system trash
	// ap.database, pos = p.readStringNulByte(pos + 2)

	fmt.Println(fmt.Sprintf(`
		authPacket: {
			length: %d
			flags: %d
			maxPacketLength: %d
			collation: %d
			filter: %d
			user: %s

			CLIENT_PROTOCOL_41: %b
			clientProtocol41: %b
		}
	`,
		ap.length,
		ap.flags,
		ap.maxPacketLength,
		ap.collation,
		ap.filter,
		ap.user,
		ap.flags&CLIENT_PROTOCOL_41,
		ap.flags&clientProtocol41,
	))

	return ap, nil
}

func (c *connection) writeFetchResult(fetchResult database.FetchResult) error {
	// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
	c.resetSequence()

	// send columnsCount packet
	{
		p := Packet{}
		p.appendBytes(0, 0, 0, c.nextSequence())
		p.appendIntegerLengthEncoded(uint64(len(fetchResult.Cols)))
		p.calculateLength()

		if _, err := c.conn.Write(p.data); err != nil {
			return err
		}
	}

	// send columns
	for i := range fetchResult.Cols {
		col := fetchResult.Cols[i]

		p := Packet{}
		p.data = []byte{0, 0, 0, c.nextSequence()}
		p.appendStringLengthEncoded("def")
		p.appendStringLengthEncoded(col.Schema)     // schema-name
		p.appendStringLengthEncoded(col.Table)      // virtual table-name
		p.appendStringLengthEncoded(col.TableAlias) // physical table-name
		p.appendStringLengthEncoded(col.Name)       // virtual column name
		p.appendStringLengthEncoded(col.Name)       // physical column name

		p.appendBytes(12)    // length of next part (0x0c)
		p.appendBytes(33, 0) // charset utf8_general_ci

		p.appendBytes(byte(len(col.Name)), 0, 0, 0) // maximum length of the field

		p.appendBytes(fieldTypeString) // type

		p.appendBytes(131, 64) // flags
		p.appendBytes(0)       // decimals
		p.appendBytes(0, 0)    // filter
		p.calculateLength()

		if _, err := c.conn.Write(p.data); err != nil {
			return err
		}
	}

	// send separated EOF
	if c.authPacket.flags&clientDeprecateEOF != clientDeprecateEOF {
		if err := c.writeEOF(response{}); err != nil {
			return err
		}
	}

	// send rows
	for i := range fetchResult.Rows {
		p := Packet{}
		p.data = []byte{0, 0, 0, c.nextSequence()}
		for j := range fetchResult.Rows[i] {
			p.appendStringLengthEncoded(fetchResult.Rows[i][j])
		}
		p.calculateLength()

		if _, err := c.conn.Write(p.data); err != nil {
			return err
		}
	}

	res := response{status: statusLastRowSent}

	// If the CLIENT_DEPRECATE_EOF client capability flag is set, OK_Packet; else EOF_Packet.
	if c.authPacket.flags&clientDeprecateEOF == clientDeprecateEOF {
		return c.writeOkPacket(res, iEOF)
	}

	return c.writeEOF(res)
}

func (c *connection) writeExecResult(execResult database.ExecResult) error {
	return c.writeOkPacket(response{
		affectedRows: execResult.RowsAffected,
		lastInsertId: execResult.LastInsertId,
	}, iOK)
}

func (c *connection) writeOkPacket(res response, cmd byte) error {
	p := Packet{}
	p.appendBytes(0, 0, 0, c.nextSequence(), cmd)
	p.appendIntegerLengthEncoded(uint64(res.affectedRows))
	p.appendIntegerLengthEncoded(uint64(res.lastInsertId))

	if c.authPacket.flags&clientProtocol41 == clientProtocol41 {
		p.appendBytes(byte(res.status), byte(res.status>>8))
		p.appendBytes(byte(res.warningsCount), byte(res.warningsCount>>8))
	} else if c.authPacket.flags&clientTransactions == clientTransactions {
		p.appendBytes(byte(res.status), byte(res.status>>8))
	}

	if c.authPacket.flags&clientSessionTrack == clientSessionTrack {
		//p.appendStringLengthEncoded("some human readable status information")
		//
		//if res.status&statusSessionStateChanged == statusSessionStateChanged {
		//	p.appendStringLengthEncoded("session state info")
		//}
	} else {
		//p.appendBytes([]byte("EOF")...)
	}

	p.calculateLength()

	_, err := c.conn.Write(p.data)
	return err
}

func (c *connection) writeEOF(res response) error {
	p := Packet{}
	p.appendBytes(0, 0, 0, c.nextSequence(), iEOF)
	if c.authPacket.flags&clientProtocol41 == clientProtocol41 {
		p.appendBytes(byte(res.warningsCount), byte(res.warningsCount>>8))
		p.appendBytes(byte(res.status), byte(res.status>>8))
	}
	p.calculateLength()

	_, err := c.conn.Write(p.data)
	return err
}

func (c *connection) writeError(dbError database.DbError) error {
	p := Packet{}
	p.appendBytes(0, 0, 0, c.nextSequence(), iERR)
	p.appendBytes(byte(dbError.Code), byte(dbError.Code>>8))

	if c.authPacket.flags&clientProtocol41 == clientProtocol41 {
		// sql_state_marker
		p.appendBytes(0x23)
		// sql_state
		p.appendBytes([]byte("42000")...)
	}

	p.appendStringNulByte(dbError.Error())
	p.calculateLength()

	_, err := c.conn.Write(p.data)
	return err
}
