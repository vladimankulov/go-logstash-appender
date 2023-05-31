## Simple pure golang implementation of logstash appender

## Usage

Call this when you want to user logger

```go
logger.GetLogger()
```

Will close the pool and release buffers

#### NOTE: use this func when application finishes

```go
defer logger.ClosePool()
```

### To run logger and send logs to logstash consider to configure logstash.json file as you wish and place it to context directory