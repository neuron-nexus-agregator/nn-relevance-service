package config

type Config struct {
	w1  float64
	w2  float64
	w3  float64
	w4  float64
	phy float64
}

func New() *Config {
	return &Config{
		w1:  0.2,
		w2:  0.3,
		w3:  0.4,
		w4:  0.1,
		phy: 0.2,
	}
}

func (c *Config) W1() float64 {
	return c.w1
}

func (c *Config) W2() float64 {
	return c.w2
}

func (c *Config) W3() float64 {
	return c.w3
}

func (c *Config) W4() float64 {
	return c.w4
}

func (c *Config) Phy() float64 {
	return c.phy
}
