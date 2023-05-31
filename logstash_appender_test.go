package go_logstash_appender

import (
	"github.com/stretchr/testify/assert"
	"net"
	"sync"
	"testing"
	"time"
)

func TestNewPoolWithEmptyHostsAndFatalError(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code did panic %s", r)
		}
	}()

	config := &LogstashConfig{
		Hosts:               "",
		Protocol:            "tcp",
		BufferSize:          8192,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}
	var err error
	assert.NotPanics(t, func() {
		err = config.createPoolOfZapLogger()
	}, "should not panic when creating pool of appender when no hosts are present")
	assert.NoError(t, err, "should not get any errors on creating pool of appender when no hosts are present")
}

func TestConnectionOnUnknownHost(t *testing.T) {
	config := &LogstashConfig{
		Hosts:               "127.0.0.1:1",
		Protocol:            "tcp",
		BufferSize:          8192,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}

	var logStashWriter *LogstashAppender

	assert.NotPanics(t, func() {
		logStashWriter = config.CreteLogStashAppender(config.Hosts)
	}, "should not panic on constructing logstash appender")
	assert.Nil(t, logStashWriter, "should not initiate logstash appender on non existing tcp host")
}

func TestConnectionOnKnownHost(t *testing.T) {
	listen, err := net.Listen("tcp", ":0")
	assert.NoError(t, err, "should get any available port")
	config := &LogstashConfig{
		Hosts:               listen.Addr().String()[4:len(listen.Addr().String())],
		Protocol:            "tcp",
		BufferSize:          8192,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}
	logStashWriter := config.CreteLogStashAppender(config.Hosts)

	assert.NotNil(t, logStashWriter, "should construct appender on existing host and port")
	assert.Equal(t, config.BufferSize, logStashWriter.buffer.Size(), "buffer size should be equal to configs")

	err = logStashWriter.Close()

	assert.NoError(t, err, "should close without err")
	assert.Equal(t, config.BufferSize, logStashWriter.buffer.Size(), "buffer size should be equal to size which is specified on configs which means that buffer is empty")
	assert.NotNil(t, logStashWriter.conn, "even if we closed the conn, it's should not been nil")
}

func TestWriteOnTcpThroughLogstashWithSmallBufferWriterSingleGoRoutine(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code shouldn't be panicked %s", r)
		}
	}()

	tcp, err := net.Listen("tcp", ":0")

	defer func(listen net.Listener) {
		err := listen.Close()
		assert.NoError(t, err, "should close tcp server without any err")
	}(tcp)
	assert.NoError(t, err, "should open tcp server without any err")

	config := &LogstashConfig{
		Hosts:               tcp.Addr().String()[4:len(tcp.Addr().String())],
		Protocol:            "tcp",
		BufferSize:          8,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}
	logStashWriter := config.CreteLogStashAppender(config.Hosts)

	assert.NotNil(t, logStashWriter, "appender should be created on existing tcp host and opened port")
	defer func(logStashWriter *LogstashAppender) {
		err := logStashWriter.Close()
		assert.NoError(t, err, "should close logstash appender without any err")
	}(logStashWriter)

	testMessage := []byte("some random message which should represent log")

	write, err := logStashWriter.Write(testMessage)

	assert.NoError(t, err, "should write to appender without any errors")
	assert.Equal(t, len(testMessage), write, "size of written bytes should be equal")

	receivedMessage := make([]byte, len(testMessage))

	incomingTcpChannel, err := tcp.Accept()

	assert.NoError(t, err, "should accept on tcp existing server")
	assert.NotNil(t, incomingTcpChannel, "should get conn on which we can consume messages")

	defer func(accept net.Conn) {
		err := accept.Close()
		assert.NoError(t, err, "should close incoming channel without any err")
	}(incomingTcpChannel)

	read, err := incomingTcpChannel.Read(receivedMessage)
	assert.NoError(t, err, "should read from incoming channel without any err")
	assert.Equal(t, len(testMessage), read, "should read all bytes")
	assert.Equal(t, string(testMessage), string(receivedMessage), "send message should be equal to received")
}

func TestWriteOnTcpThroughLogstashWithSmallBufferWriterMultiplyGoRoutine(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code shouldn't be panicked %s", r)
		}
	}()

	tcp, err := net.Listen("tcp", ":0")

	defer func(listen net.Listener) {
		err := listen.Close()
		assert.NoError(t, err, "should close tcp server without any err")
	}(tcp)
	assert.NoError(t, err, "should open tcp server without any err")

	config := &LogstashConfig{
		Hosts:               tcp.Addr().String()[4:len(tcp.Addr().String())],
		Protocol:            "tcp",
		BufferSize:          8,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}
	logStashWriter := config.CreteLogStashAppender(config.Hosts)

	assert.NotNil(t, logStashWriter, "appender should be created on existing tcp host and opened port")
	defer func(logStashWriter *LogstashAppender) {
		err := logStashWriter.Close()
		assert.NoError(t, err, "should close logstash appender without any err")
	}(logStashWriter)

	testMessageForOneGoRoutine := []byte("first message")
	testMessageForTwoGoRoutine := []byte("second random message log")

	var wg sync.WaitGroup
	wg.Add(1)
	go func(l *LogstashAppender, msg []byte) {
		wg.Done()
		write, err := l.Write(msg)
		assert.NoError(t, err, "should write to appender without any errors")
		assert.Equal(t, len(testMessageForOneGoRoutine), write, "size of written bytes should be equal")

	}(logStashWriter, testMessageForOneGoRoutine)
	wg.Add(1)
	go func(l *LogstashAppender, msg []byte) {
		defer wg.Done()
		write, err := l.Write(msg)
		assert.NoError(t, err, "should write to appender without any errors")
		assert.Equal(t, len(testMessageForTwoGoRoutine), write, "size of written bytes should be equal")
	}(logStashWriter, testMessageForTwoGoRoutine)

	wg.Wait()
	receivedMessage := make([]byte, 64)

	incomingTcpChannel, err := tcp.Accept()

	assert.NoError(t, err, "should accept on tcp existing server")
	assert.NotNil(t, incomingTcpChannel, "should get conn on which we can consume messages")

	defer func(accept net.Conn) {
		err := accept.Close()
		assert.NoError(t, err, "should close incoming channel without any err")
	}(incomingTcpChannel)

	err = incomingTcpChannel.SetReadDeadline(time.Now().Add(time.Second))
	assert.NoError(t, err, "should set read deadline without any err")
	read, err := incomingTcpChannel.Read(receivedMessage)

	assert.NoError(t, err, "should read from incoming channel without any err")
	assert.Equal(t, len(testMessageForTwoGoRoutine)+len(testMessageForOneGoRoutine), read, "should read all bytes")

	assert.Contains(t, string(receivedMessage), string(testMessageForOneGoRoutine), "first message should be received")
	assert.Contains(t, string(receivedMessage), string(testMessageForTwoGoRoutine), "second message should be received")
}

func TestWriteOnTcpThroughLogstashOnLostConnectionRoutine(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code shouldn't be panicked %s", r)
		}
	}()
	tcp, err := net.Listen("tcp", ":0")
	assert.NoError(t, err, "should open tcp server without any err")

	config := &LogstashConfig{
		Hosts:               tcp.Addr().String()[4:len(tcp.Addr().String())],
		Protocol:            "tcp",
		BufferSize:          8,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}
	logStashWriter := config.CreteLogStashAppender(config.Hosts)

	assert.NotNil(t, logStashWriter, "appender should be created on existing tcp host and opened port")
	defer func(logStashWriter *LogstashAppender) {
		err := logStashWriter.Close()
		assert.NoError(t, err, "should close logstash appender without any err")
	}(logStashWriter)

	err = tcp.Close()
	assert.NoError(t, err, "should not get err on closing tcp server")

	write, err := logStashWriter.Write([]byte("first message"))
	assert.NoError(t, err, "should not get any err even if tcp conn is closed")
	assert.Equal(t, 0, write, "should be written 0 bytes and lost connection")

	receivedMessage := make([]byte, 64)
	newTcp, err := net.Listen("tcp", tcp.Addr().String()[4:len(tcp.Addr().String())])

	defer func(listen net.Listener) {
		err := listen.Close()
		assert.NoError(t, err, "should close new tcp server without any err")
	}(newTcp)
	assert.NoError(t, err, "should open new tcp server without any err")

	time.Sleep(time.Second * 12)
	secondMessage := []byte("secondMessage")

	incomingTcpChannel, err := newTcp.Accept()
	assert.NoError(t, err, "should accept on tcp existing server")
	assert.NotNil(t, incomingTcpChannel, "should get conn on which we can consume messages")

	defer func(accept net.Conn) {
		err := accept.Close()
		assert.NoError(t, err, "should close incoming channel without any err")
	}(incomingTcpChannel)

	w, err := logStashWriter.Write(secondMessage)
	assert.NoError(t, err, "should write on reopened tcp connection without any err")
	assert.Equal(t, len(secondMessage), w, "size of written bytes should be equal")

	err = incomingTcpChannel.SetReadDeadline(time.Now().Add(time.Second))
	assert.NoError(t, err, "should set read deadline without any err")

	read, err := incomingTcpChannel.Read(receivedMessage)
	assert.NoError(t, err, "should read incoming bytes without any err")
	assert.Equal(t, len(secondMessage), read, "read bytes should be equal to the message size")
	assert.Contains(t, string(receivedMessage), string(secondMessage), "received message should contain sent message")
}
