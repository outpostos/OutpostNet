package protocol

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

// Codec handles reading and writing Frames on a net.Conn.
// Reads must be done from a single goroutine. Writes are thread-safe.
type Codec struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	wmu    sync.Mutex
}

// NewCodec wraps a net.Conn with buffered frame-level read/write.
func NewCodec(conn net.Conn) *Codec {
	return &Codec{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, 32*1024),
		writer: bufio.NewWriterSize(conn, 32*1024),
	}
}

// ReadFrame reads one complete frame from the connection.
func (c *Codec) ReadFrame() (*Frame, error) {
	var length uint32
	if err := binary.Read(c.reader, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	if length < FrameHeaderSize {
		return nil, errors.New("frame too small")
	}
	payloadLen := length - FrameHeaderSize
	if payloadLen > MaxFramePayload {
		return nil, fmt.Errorf("frame payload too large: %d bytes", payloadLen)
	}

	var msgType uint8
	if err := binary.Read(c.reader, binary.BigEndian, &msgType); err != nil {
		return nil, err
	}
	var streamID uint32
	if err := binary.Read(c.reader, binary.BigEndian, &streamID); err != nil {
		return nil, err
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			return nil, err
		}
	}

	return &Frame{
		Type:     MsgType(msgType),
		StreamID: streamID,
		Payload:  payload,
	}, nil
}

// WriteFrame writes one complete frame to the connection. Thread-safe.
func (c *Codec) WriteFrame(f *Frame) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	length := uint32(FrameHeaderSize + len(f.Payload))
	if err := binary.Write(c.writer, binary.BigEndian, length); err != nil {
		return err
	}
	if err := binary.Write(c.writer, binary.BigEndian, uint8(f.Type)); err != nil {
		return err
	}
	if err := binary.Write(c.writer, binary.BigEndian, f.StreamID); err != nil {
		return err
	}
	if len(f.Payload) > 0 {
		if _, err := c.writer.Write(f.Payload); err != nil {
			return err
		}
	}
	return c.writer.Flush()
}

// Close closes the underlying connection.
func (c *Codec) Close() error {
	return c.conn.Close()
}
