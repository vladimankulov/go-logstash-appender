package go_logstash_appender

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"sync"
)

type LoggersPool struct {
	storage    *sync.Pool
	hosts      []string
	cHostIndex int
	mtx        sync.Mutex
}

func (c *LoggersPool) getHost() string {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.hosts == nil {
		return ""
	}

	if c.cHostIndex >= len(c.hosts)-1 {
		c.cHostIndex = 0
	} else {
		c.cHostIndex += 1
	}
	return c.hosts[c.cHostIndex]
}

var zapLoggerPool *LoggersPool

func GetLogger() *zap.Logger {
	if zapLoggerPool == nil {
		readConfigAndInitLoggers()
	}

	if el := zapLoggerPool.storage.Get(); el != nil {
		logger := el.(*zap.Logger)
		zapLoggerPool.storage.Put(el)
		return logger
	}
	return nil
}

func ClosePool() {
	if zapLoggerPool != nil {
		//set pool New function to nil so whenever pool exceed connection, it does not create new one
		zapLoggerPool.storage.New = nil
		for {
			if el := zapLoggerPool.storage.Get(); el != nil {
				logger := el.(*zap.Logger)
				err := logger.Sync()
				if err != nil {
					return
				}
			}
		}
	}
}

func initLogger(logStashAppender *LogstashAppender) *zap.Logger {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.ISO8601TimeEncoder
	config.TimeKey = "timestamp"
	config.MessageKey = "message"
	consoleEncoder := zapcore.NewConsoleEncoder(config)
	defaultLogLevel := zapcore.InfoLevel

	if logStashAppender != nil {
		logStashEncoder := zapcore.NewJSONEncoder(config)
		writer := zapcore.AddSync(logStashAppender)
		core := zapcore.NewTee(
			zapcore.NewCore(logStashEncoder, writer, defaultLogLevel),
			zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), defaultLogLevel),
		)

		return zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	} else {
		fmt.Println("No logstash tcp appender was provided, so console appender will be used")
		core := zapcore.NewTee(
			zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), defaultLogLevel),
		)
		return zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	}
}
