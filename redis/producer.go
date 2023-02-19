package redis

type (
	Producer struct {
		*Redis
	}
)

func NewProducer(url string) (*Producer, error) {
	r, err := newRedis(url)
	if err != nil {
		return nil, err
	}

	return &Producer{Redis: r}, nil
}

func (r *Producer) Write(p []byte) (int, error) {
	err := r.xadd("event_type", "log", "bytes", string(p))
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func (r *Producer) Close() error {
	_ = r.xadd("event_type", "disconnect")

	return r.Redis.Close()
}
