package nats_streaming

import (
	"fmt"
	"log"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal/tls"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"
	nats_client "github.com/nats-io/nats.go"
	stan_client "github.com/nats-io/stan.go"
)

type NATS struct {
	Servers   []string `toml:"servers"`
	Secure    bool     `toml:"secure"`
	Username  string   `toml:"username"`
	Password  string   `toml:"password"`
	Subject   string   `toml:"subject"`
	ClientId  string   `toml:"client_id"`
	ClusterId string   `toml:"cluster_id"`
	tls.ClientConfig

	conn       *nats_client.Conn
	stan       stan_client.Conn
	serializer serializers.Serializer
}

var sampleConfig = `
  ## URLs of NATS servers
  servers = ["nats://localhost:4222"]
  ## Optional credentials
  # username = ""
  # password = ""
  ## NATS subject for producer messages
  subject = "telegraf"
  cluster_id = "test-cluster"
  client_id = "test-client"

  ## Use Transport Layer Security
  # secure = false

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## Data format to output.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  data_format = "influx"
`

func (n *NATS) SetSerializer(serializer serializers.Serializer) {
	n.serializer = serializer
}

func (n *NATS) Connect() error {
	var err error

	// set default NATS connection options
	opts := nats_client.DefaultOptions

	// override max reconnection tries
	opts.MaxReconnect = -1

	// override servers, if any were specified
	opts.Servers = n.Servers

	// override authentication, if any was specified
	if n.Username != "" {
		opts.User = n.Username
		opts.Password = n.Password
	}

	if n.Secure {
		tlsConfig, err := n.ClientConfig.TLSConfig()
		if err != nil {
			return err
		}

		opts.Secure = true
		opts.TLSConfig = tlsConfig
	}

	// try and connect
	n.conn, err = opts.Connect()
	if err != nil {
		return err
	}
	n.stan, err = stan_client.Connect(n.ClusterId, n.ClientId, stan_client.NatsConn(n.conn))

	return err
}

func (n *NATS) Close() error {
	n.stan.Close()
	n.conn.Close()
	return nil
}

func (n *NATS) SampleConfig() string {
	return sampleConfig
}

func (n *NATS) Description() string {
	return "Send telegraf measurements to NATS"
}

func (n *NATS) Write(metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	buf, err := n.serializer.SerializeBatch(metrics)
	if err != nil {
		log.Printf("D! [outputs.nats] Could not serialize metric: %v", err)
		return nil
	}

	err = n.stan.Publish(n.Subject, buf)
	if err != nil {
		return fmt.Errorf("FAILED to send NATS message: %v", err.Error())
	}

	return nil
}

func init() {
	outputs.Add("nats_streaming", func() telegraf.Output {
		return &NATS{}
	})
}
