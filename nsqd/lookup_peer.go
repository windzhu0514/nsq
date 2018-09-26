package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/windzhu0514/go-nsq"
	"github.com/windzhu0514/nsq/internal/lg"
)

// lookupPeer 是个底层类型，用来连接、读取、写入nsqlookupd
// 一个lookupPeer实例用来惰性连接nsqlookupd和优雅的重连（也就是说这个库处理了所有的相关操作）。
// client 可以简单使用命令接口进行数据传输
// lookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
//
// A lookupPeer instance is designed to connect lazily to nsqlookupd and reconnect
// gracefully (i.e. it is all handled by the library).  Clients can simply use the
// Command interface to perform a round-trip.
type lookupPeer struct {
	logf            lg.AppLogFunc
	addr            string
	conn            net.Conn
	state           int32
	connectCallback func(*lookupPeer)
	maxBodySize     int64
	Info            peerInfo
}

// peerInfo contains metadata for a lookupPeer instance (and is JSON marshalable)
type peerInfo struct {
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	BroadcastAddress string `json:"broadcast_address"`
}

// 实例每次连接时都会调用提供的connectCallback函数
// newLookupPeer creates a new lookupPeer instance connecting to the supplied address.
//
// The supplied connectCallback will be called *every* time the instance connects.
func newLookupPeer(addr string, maxBodySize int64, l lg.AppLogFunc, connectCallback func(*lookupPeer)) *lookupPeer {
	return &lookupPeer{
		logf:            l,
		addr:            addr,
		state:           stateDisconnected,
		maxBodySize:     maxBodySize,
		connectCallback: connectCallback,
	}
}

// Connect will Dial the specified address, with timeouts
func (lp *lookupPeer) Connect() error {
	lp.logf(lg.INFO, "LOOKUP connecting to %s", lp.addr)
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	lp.conn = conn
	return nil
}

// String returns the specified address
func (lp *lookupPeer) String() string {
	return lp.addr
}

// Read implements the io.Reader interface, adding deadlines
func (lp *lookupPeer) Read(data []byte) (int, error) {
	lp.conn.SetReadDeadline(time.Now().Add(time.Second))
	return lp.conn.Read(data)
}

// Write implements the io.Writer interface, adding deadlines
func (lp *lookupPeer) Write(data []byte) (int, error) {
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return lp.conn.Write(data)
}

// Close implements the io.Closer interface
func (lp *lookupPeer) Close() error {
	lp.state = stateDisconnected
	if lp.conn != nil {
		return lp.conn.Close()
	}
	return nil
}

// Command performs a round-trip for the specified Command.
//
// It will lazily connect to nsqlookupd and gracefully handle
// reconnecting in the event of a failure.
//
// It returns the response from nsqlookupd as []byte
func (lp *lookupPeer) Command(cmd *nsq.Command) ([]byte, error) {
	initialState := lp.state
	if lp.state != stateConnected {
		err := lp.Connect()
		if err != nil {
			return nil, err
		}
		lp.state = stateConnected
		_, err = lp.Write(nsq.MagicV1)
		if err != nil {
			lp.Close()
			return nil, err
		}
		// 连接前是断开状态 调用连接回调函数
		if initialState == stateDisconnected {
			lp.connectCallback(lp)
		}
		if lp.state != stateConnected {
			return nil, fmt.Errorf("lookupPeer connectCallback() failed")
		}
	}
	if cmd == nil {
		return nil, nil
	}
	_, err := cmd.WriteTo(lp) // 发送命令
	if err != nil {
		lp.Close()
		return nil, err
	}
	resp, err := readResponseBounded(lp, lp.maxBodySize)
	if err != nil {
		lp.Close()
		return nil, err
	}
	return resp, nil
}

func readResponseBounded(r io.Reader, limit int64) ([]byte, error) {
	var msgSize int32

	// message size 读取返回的消息体长度
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	if int64(msgSize) > limit {
		return nil, fmt.Errorf("response body size (%d) is greater than limit (%d)",
			msgSize, limit)
	}

	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
