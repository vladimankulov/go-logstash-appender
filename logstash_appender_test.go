package go_logstash_appender

import (
	"fmt"
	"net"
	"strings"
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
	_ = config.createPoolOfZapLogger()
}

func TestNewPoolWithEmptyHostsAndWithoutFatalError(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code shouldn't be panicked %s", r)
		}
	}()

	config := &LogstashConfig{
		Hosts:               "",
		Protocol:            "tcp",
		BufferSize:          8192,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}
	_ = config.createPoolOfZapLogger()
}

func TestConnectionOnUnknownHost(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code shouldn't be panicked %s", r)
		}
	}()

	config := &LogstashConfig{
		Hosts:               "127.0.0.1:9092",
		Protocol:            "tcp",
		BufferSize:          8192,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}
	logStashWriter := config.CreteLogStashAppender(config.Hosts)

	if logStashWriter != nil {
		t.Errorf("logstash should be created on nonexisting tcp host")
	}
}

func TestConnectionOnKnownHost(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code should be panicked %s", r)
		}
	}()

	listen, err := net.Listen("tcp", ":9092")

	defer func(listen net.Listener) {
		err := listen.Close()
		if err != nil {
			t.Errorf("cannot close tcp port: %s", err)
		}
	}(listen)

	if err != nil {
		t.Errorf("cannot open tcp port: %s", err)
		return
	}

	config := &LogstashConfig{
		Hosts:               ":9092",
		Protocol:            "tcp",
		BufferSize:          8192,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}
	logStashWriter := config.CreteLogStashAppender(config.Hosts)

	err = logStashWriter.Close()
	if err != nil {
		fmt.Println("unable to close logstashWriter: ", err.Error())
		return
	}

	if logStashWriter == nil {
		t.Errorf("logstash should be created on existing tcp host and opened tcp port 9092")
	}
}

func TestWriteOnTcpThroughLogstashWithSmallBufferWriterSingleGoRoutine(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code shouldn't be panicked %s", r)
		}
	}()

	tcp, err := net.Listen("tcp", ":9092")

	defer func(listen net.Listener) {
		err := listen.Close()
		if err != nil {
			t.Errorf("cannot close tcp port: %s", err)
		}
	}(tcp)

	if err != nil {
		t.Errorf("cannot open tcp port: %s", err)
		return
	}

	config := &LogstashConfig{
		Hosts:               ":9092",
		Protocol:            "tcp",
		BufferSize:          8,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}
	logStashWriter := config.CreteLogStashAppender(config.Hosts)

	if logStashWriter == nil {
		t.Errorf("logstash should be created on existing tcp host and opened port 9092")
	}

	defer func(logStashWriter *LogstashAppender) {
		err := logStashWriter.Close()
		if err != nil {
			t.Error("unable to close logstashWriter: ", err.Error())
		}
	}(logStashWriter)

	testMessage := []byte("some random message which should represent log")

	write, err := logStashWriter.Write(testMessage)

	if err != nil || write != len(testMessage) {
		t.Errorf("unable to write on opened tcp connection cause: %s", err.Error())
	}
	receivedMessage := make([]byte, len(testMessage))

	incomingTcpChannel, err := tcp.Accept()
	if err != nil {
		t.Errorf("cannot accept message trough tcp port: %s", err)
		return
	}

	defer func(accept net.Conn) {
		err := accept.Close()
		if err != nil {
			t.Errorf("cannot close reader channel: %s", err)
		}
	}(incomingTcpChannel)

	read, err := incomingTcpChannel.Read(receivedMessage)
	if err != nil || read != len(testMessage) {
		t.Errorf("unable to read on opened tcp connection cause: %s", err.Error())
		return
	}

	if string(testMessage) != string(receivedMessage) {
		t.Errorf("%s not equal to %s", string(testMessage), string(receivedMessage))
	}
}

func TestWriteOnTcpThroughLogstashWithSmallBufferWriterMultiplyGoRoutine(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code shouldn't be panicked %s", r)
		}
	}()

	tcp, err := net.Listen("tcp", ":9092")

	defer func(listen net.Listener) {
		err := listen.Close()
		if err != nil {
			t.Errorf("cannot close tcp port: %s", err)
		}
	}(tcp)

	if err != nil {
		t.Errorf("cannot open tcp port: %s", err)
		return
	}

	config := &LogstashConfig{
		Hosts:               ":9092",
		Protocol:            "tcp",
		BufferSize:          8,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}
	logStashWriter := config.CreteLogStashAppender(config.Hosts)

	if logStashWriter == nil {
		t.Errorf("logstash should be created on existing tcp host and opened port 9092")
	}

	defer func(logStashWriter *LogstashAppender) {
		err := logStashWriter.Close()
		if err != nil {
			t.Error("unable to close logstashWriter: ", err.Error())
		}
	}(logStashWriter)

	testMessageForOneGoRoutine := []byte("first message")
	testMessageForTwoGoRoutine := []byte("second random message log")

	var wg sync.WaitGroup
	go func(l *LogstashAppender, msg []byte) {
		wg.Add(1)
		wg.Done()
		write, err := l.Write(msg)
		if err != nil || write != len(msg) {
			t.Errorf("unable to write on opened tcp connection cause: %s", err.Error())
			return
		}
		return
	}(logStashWriter, testMessageForOneGoRoutine)

	go func(l *LogstashAppender, msg []byte) {
		wg.Add(1)
		defer wg.Done()
		write, err := l.Write(msg)
		if err != nil || write != len(msg) {
			t.Errorf("unable to write on opened tcp connection cause: %s", err.Error())
			return
		}
		return
	}(logStashWriter, testMessageForTwoGoRoutine)

	wg.Wait()
	receivedMessage := make([]byte, 64)

	incomingTcpChannel, err := tcp.Accept()
	if err != nil {
		t.Errorf("cannot accept message trough tcp port: %s", err)
		return
	}

	defer func(accept net.Conn) {
		err := accept.Close()
		if err != nil {
			t.Errorf("cannot close reader channel: %s", err)
		}
	}(incomingTcpChannel)

	read, err := incomingTcpChannel.Read(receivedMessage)
	if err != nil || read == 0 {
		t.Errorf("unable to read on opened tcp connection cause: %s", err.Error())
		return
	}

	if !strings.Contains(string(receivedMessage), string(testMessageForOneGoRoutine)) && !strings.Contains(string(receivedMessage), string(testMessageForTwoGoRoutine)) {
		t.Errorf("haven't received all messages, read: '%s' first message: '%s' second message: '%s'", string(receivedMessage), string(testMessageForOneGoRoutine), string(testMessageForTwoGoRoutine))
	}
}

func TestWriteOnTcpThroughLogstashOnLostConnectionRoutine(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("The code shouldn't be panicked %s", r)
		}
	}()

	tcp, err := net.Listen("tcp", ":9092")
	defer func(tcp net.Listener) {
		err := tcp.Close()
		if e, ok := err.(*net.OpError); ok {
			if e.Op != "close" {
				t.Errorf("first connection wasn't closed, so test didn't provide full quilified environment %s", e.Err)
			}
		}
	}(tcp)

	if err != nil {
		t.Errorf("cannot open tcp port: %s", err)
		return
	}

	config := &LogstashConfig{
		Hosts:               ":9092",
		Protocol:            "tcp",
		BufferSize:          8,
		ConnectionTimeout:   20,
		ReconnectionTimeout: 10,
	}
	logStashWriter := config.CreteLogStashAppender(config.Hosts)

	if logStashWriter == nil {
		t.Errorf("logstash should be created on existing tcp host and opened port 9092")
	}

	defer func(logStashWriter *LogstashAppender) {
		err := logStashWriter.Close()
		if err != nil {
			t.Error("unable to close logstashWriter: ", err.Error())
		}
	}(logStashWriter)

	err = tcp.Close()
	if err != nil {
		t.Errorf("unable to perform close tcp while testing case: %s", err.Error())
		return
	}

	_, err = logStashWriter.Write([]byte("first message"))
	if err != nil {
		t.Errorf("error_builder should be thrown even if tcp connection is closed: %s", err.Error())
		return
	}

	receivedMessage := make([]byte, 64)
	newTcp, err := net.Listen("tcp", ":9092")

	defer func(tcp net.Listener) {
		err := tcp.Close()
		if err != nil {
			t.Errorf("cannot close second tcp connection: %s", err)
		}
	}(newTcp)
	if err != nil {
		_ = logStashWriter.Close()
		t.Errorf("uanble to reopen tcp port for test: %s", err.Error())
	}
	time.Sleep(time.Second * 12)
	secondMessage := []byte("secondMessage")

	incomingTcpChannel, err := newTcp.Accept()
	if err != nil {
		t.Errorf("cannot accept message trough tcp port: %s", err)
	}

	w, err := logStashWriter.Write(secondMessage)
	if err != nil && w != len(secondMessage) {
		t.Errorf("should write message cause we open tcp again and there has to be no error_builder: %s", err.Error())
	}

	defer func(accept net.Conn) {
		err := accept.Close()
		if err != nil {
			t.Errorf("cannot close reader channel: %s", err)
		}
	}(incomingTcpChannel)
	err = incomingTcpChannel.SetReadDeadline(time.Now().Add(time.Second * 2))
	if err != nil {
		t.Errorf("cannot set read dead line on tcp connect")
		return
	}

	read, err := incomingTcpChannel.Read(receivedMessage)
	if err != nil || read == 0 {
		t.Errorf("unable to read on opened tcp connection cause: %s", err.Error())
	}

	if !strings.Contains(string(receivedMessage), string(secondMessage)) {
		t.Errorf("haven't received all messages, read: '%s' first message: '%s'", string(receivedMessage), string(secondMessage))
	}
}
