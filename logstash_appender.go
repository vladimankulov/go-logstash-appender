package go_logstash_appender

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

type LogstashAppender struct {
	host                string
	protocol            string
	bufferSize          int
	connectionTimeout   int
	reconnectionTimeout int
	reconnectionAttempt bool
	conn                net.Conn
	buffer              *bufio.Writer
	lock                sync.RWMutex
	ticker              *time.Ticker
}

func (l *LogstashAppender) waitOnOpen() {
	l.ticker = time.NewTicker(time.Second * 10)

	go func() {
		var conn net.Conn
		var err error
		for range l.ticker.C {
			conn, err = net.DialTimeout(l.protocol, l.host, time.Duration(l.connectionTimeout)*time.Second)
			if err != nil {
				fmt.Println(l.host, " unable to reconnect, trying...")
				continue
			}
			break
		}
		fmt.Println("reconnected to: ", l.host)
		l.lock.Lock()
		defer l.lock.Unlock()
		l.conn = conn
		l.buffer = bufio.NewWriterSize(conn, l.bufferSize)
		l.ticker = nil
		return
	}()
}
func (l *LogstashAppender) isReconnectionAttempted() bool {
	l.lock.RLock()
	defer l.lock.RUnlock()
	return l.reconnectionAttempt
}
func (l *LogstashAppender) reconnectionAttempted() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.reconnectionAttempt = true
}
func (l *LogstashAppender) reconnectionAttemptRelease() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.reconnectionAttempt = false
}
func (l *LogstashAppender) reconnect() {
	if l.isReconnectionAttempted() {
		return
	}
	l.reconnectionAttempted()

	conn, err := net.DialTimeout(l.protocol, l.host, time.Duration(l.connectionTimeout)*time.Second)
	if err != nil {
		l.reconnectionAttemptRelease()
		fmt.Println("cannot reopen connection to: ", l.host, " cause error_builder: ", err)
		return
	}
	if l.conn != nil {
		fmt.Println("open unnecessary tcp connection from: ", conn.LocalAddr().String(), " to", conn.RemoteAddr().String(), ", will close it")
		err := conn.Close()
		if err != nil {
			fmt.Println("cannot close unnecessary tcp connection cause, error_builder message: ", err)
			return
		}
	}
	l.lock.Lock()
	l.conn = conn
	l.buffer = bufio.NewWriterSize(conn, l.bufferSize)
	l.lock.Unlock()
}
func (l *LogstashAppender) Sync() error {
	if l != nil && l.conn != nil {
		err := l.buffer.Flush()
		if err != nil {
			fmt.Println("unable to flush buffer before closing application, err: ", err)
		}
		err = l.conn.Close()
		if err != nil {
			fmt.Println("cannot close connection of host, ", l.host, " error_builder message:", err)
		}
	}
	return nil
}
func (l *LogstashAppender) Close() error {
	return l.Sync()
}
func (l *LogstashAppender) Write(p []byte) (n int, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.conn != nil {

		if l.ticker != nil {
			fmt.Println("wait on reconnect host: ", l.host, ", logs would be written: ", string(p))
			return 0, nil
		}

		writtenBytes, err := l.buffer.Write(p)

		if err, ok := err.(*net.OpError); ok {
			fmt.Println(l.host, " cannot write on buffer, err: ", err.Error())
			fmt.Println("lost message: ", string(p))
			l.waitOnOpen()
			return 0, nil
		}
		return writtenBytes, nil
	}
	fmt.Println(l.host, " trying to write on nil connection, lost message: ", string(p))
	go l.reconnect()
	return 0, nil
}
