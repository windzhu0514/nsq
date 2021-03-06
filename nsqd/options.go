// nsqd所有配置信息
package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"github.com/nsqio/go-nsq/internal/lg"
)

type Options struct {
	// basic options
	ID        int64  `flag:"node-id" cfg:"id"` // nsqd节点id 最大1024
	LogLevel  string `flag:"log-level"`
	LogPrefix string `flag:"log-prefix"`
	Verbose   bool   `flag:"verbose"` // for backwards compatibility
	Logger    Logger
	logLevel  lg.LogLevel // private, not really an option

	TCPAddress               string        `flag:"tcp-address"`                                        // tcp监听地址
	HTTPAddress              string        `flag:"http-address"`                                       // http监听地址
	HTTPSAddress             string        `flag:"https-address"`                                      // https监听地址
	BroadcastAddress         string        `flag:"broadcast-address"`                                  // 注册到lookupd的地址 默认是hostname
	NSQLookupdTCPAddresses   []string      `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"` // nsqlookupd
	AuthHTTPAddresses        []string      `flag:"auth-http-address" cfg:"auth_http_addresses"`
	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout" cfg:"http_client_connect_timeout"`
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout"`

	// diskqueue options
	DataPath        string        `flag:"data-path"`          // 缓存消息的磁盘路径
	MemQueueSize    int64         `flag:"mem-queue-size"`     // 内存消息队列可容纳的消息数量
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"` // 保存消息的文件最大字节数
	SyncEvery       int64         `flag:"sync-every"`         // 磁盘队列读和写每处理多少个消息同步状态到文件
	SyncTimeout     time.Duration `flag:"sync-timeout"`       // 定时同步状态到文件

	QueueScanInterval        time.Duration
	QueueScanRefreshInterval time.Duration
	QueueScanSelectionCount  int
	QueueScanWorkerPoolMax   int
	QueueScanDirtyPercent    float64

	// msg and command options
	MsgTimeout    time.Duration `flag:"msg-timeout"`
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"`
	MaxMsgSize    int64         `flag:"max-msg-size"`
	MaxBodySize   int64         `flag:"max-body-size"`
	MaxReqTimeout time.Duration `flag:"max-req-timeout"`
	ClientTimeout time.Duration

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`
	MaxRdyCount            int64         `flag:"max-rdy-count"` // 一个client最大的ready次数
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"`

	// statsd integration
	StatsdAddress       string        `flag:"statsd-address"`         // nsqd状态的推送地址 为空不开启推送
	StatsdPrefix        string        `flag:"statsd-prefix"`          // 状态信息前缀
	StatsdInterval      time.Duration `flag:"statsd-interval"`        // 推送nsqd状态的间隔
	StatsdMemStats      bool          `flag:"statsd-mem-stats"`       // 是否推送nsqd内存和gc状态信息
	StatsdUDPPacketSize int           `flag:"statsd-udp-packet-size"` // 缓存状态信息的buff大小

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"`

	// TLS config
	TLSCert             string `flag:"tls-cert"`
	TLSKey              string `flag:"tls-key"`
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"`
	TLSRootCAFile       string `flag:"tls-root-ca-file"`
	TLSRequired         int    `flag:"tls-required"` // 客户端是否需要TLS连接(true, false, tcp-https)
	TLSMinVersion       uint16 `flag:"tls-min-version"`

	// compression
	DeflateEnabled  bool `flag:"deflate"`
	MaxDeflateLevel int  `flag:"max-deflate-level"`
	SnappyEnabled   bool `flag:"snappy"`
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:        defaultID,
		LogPrefix: "[nsqd] ",
		LogLevel:  "info",

		TCPAddress:       "0.0.0.0:4150",
		HTTPAddress:      "0.0.0.0:4151",
		HTTPSAddress:     "0.0.0.0:4152",
		BroadcastAddress: hostname,

		NSQLookupdTCPAddresses: make([]string, 0),
		AuthHTTPAddresses:      make([]string, 0),

		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanSelectionCount:  20,
		QueueScanWorkerPoolMax:   4,
		QueueScanDirtyPercent:    0.25,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024, // 一个消息最大1M
		MaxBodySize:   5 * 1024 * 1024,
		MaxReqTimeout: 1 * time.Hour,
		ClientTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 1 * time.Second,

		StatsdPrefix:        "nsq.%s",
		StatsdInterval:      60 * time.Second,
		StatsdMemStats:      true,
		StatsdUDPPacketSize: 508,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,
	}
}
