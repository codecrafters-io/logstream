package redis

type RedisWriter struct {
	client *redisClient
}

func NewRedisWriter(streamUrl string) (*RedisWriter, error) {
	client, err := newRedisClient(streamUrl)
	if err != nil {
		return nil, err
	}

	return &RedisWriter{client: client}, nil
}

func (r *RedisWriter) Write(p []byte) (int, error) {
	err := r.client.xadd("event_type", "log", "bytes", string(p))
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func (r *RedisWriter) Close() error {
	err := r.client.xadd("event_type", "disconnect")
	if err != nil {
		return err
	}

	return r.client.close()
}
