package go_logstash_appender

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	tcp                        = "tcp"
	udp                        = "udp"
	defaultReconnectionTimeout = 20
	defaultConnectionTimeout   = 10
	defaultBufferSize          = 8192
)

type LogstashConfig struct {
	Profile             string `yaml:"profile" json:"profile" xml:"profile"`
	Hosts               string `yaml:"hosts" json:"hosts" xml:"hosts"`
	Protocol            string `yaml:"protocol" json:"protocol" xml:"protocol"`
	BufferSize          int    `yaml:"bufferSize" json:"bufferSize" xml:"bufferSize"`
	ConnectionTimeout   int    `yaml:"connectionTimeout" json:"connectionTimeout" xml:"connectionTimeout"`
	ReconnectionTimeout int    `yaml:"reconnectionTimeout" json:"reconnectionTimeout" xml:"reconnectionTimeout"`
}

func readConfigAndInitLoggers() {
	yamFile, err := os.ReadFile("logstash.json")

	config := &LogstashConfig{}

	if err == nil {
		err = json.Unmarshal(yamFile, config)
	} else {
		fmt.Println("Unable to read logstash yaml file: ", err)
	}

	if err != nil {
		config.Profile = "dev"
		fmt.Println("Unable to unmarshal logstash yaml file: ", err)
	}
	_ = config.createPoolOfZapLogger()
}

func (c *LogstashConfig) createPoolOfZapLogger() error {
	if c.isProductiveEnvironment() {
		if c.isHostsAreDeclared() {
			c.setDefaultParamsIfMissing()

			hosts := strings.Split(c.Hosts, ",")
			zapLoggerPool = &LoggersPool{storage: &sync.Pool{}, hosts: hosts, cHostIndex: 0}
			for _, h := range hosts {
				zapLoggerPool.storage.Put(initLogger(c.CreteLogStashAppender(h)))
			}
			zapLoggerPool.storage.New = func() any {
				return initLogger(c.CreteLogStashAppender(zapLoggerPool.getHost()))
			}
		} else {
			panic("error_builder: logstash host are empty, declare logstash hosts or change environment")
		}
	} else {
		zapLoggerPool = &LoggersPool{storage: &sync.Pool{}}
		for i := 0; i < 2; i++ {
			zapLoggerPool.storage.Put(initLogger(nil))
		}
		zapLoggerPool.storage.New = func() any {
			return initLogger(c.CreteLogStashAppender(zapLoggerPool.getHost()))
		}
	}
	return nil
}

func (c *LogstashConfig) CreteLogStashAppender(host string) *LogstashAppender {
	if host == "" {
		fmt.Println("unable to create logstash appender cause host is nil: ", host)
		return nil
	}

	conn, err := net.DialTimeout(c.Protocol, host, time.Duration(c.ConnectionTimeout)*time.Second)
	if err != nil {
		fmt.Println("cannot open connection to: ", host, " cause error_builder: ", err)
		return nil
	}
	return &LogstashAppender{
		host:                host,
		protocol:            c.Protocol,
		bufferSize:          c.BufferSize,
		connectionTimeout:   c.ConnectionTimeout,
		reconnectionTimeout: c.ReconnectionTimeout,
		conn:                conn,
		buffer:              bufio.NewWriterSize(conn, c.BufferSize),
	}
}

func (c *LogstashConfig) isProductiveEnvironment() bool {
	return strings.Contains(c.Profile, "prod")
}
func (c *LogstashConfig) isHostsAreDeclared() bool {
	return c.Hosts != ""
}

func (c *LogstashConfig) setDefaultParamsIfMissing() {
	if c.Profile == "" {
		c.Profile = "dev"
	}
	if udp != c.Protocol {
		c.Protocol = tcp
	}
	if c.ConnectionTimeout == 0 {
		c.ConnectionTimeout = defaultConnectionTimeout
	}
	if c.BufferSize == 0 {
		c.BufferSize = defaultBufferSize
	}
	if c.ReconnectionTimeout == 0 {
		c.ReconnectionTimeout = defaultReconnectionTimeout
	}
}
