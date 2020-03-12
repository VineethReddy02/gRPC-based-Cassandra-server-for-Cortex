package main

import (
	"context"
	"cortex-cassandra-store/grpc"
	"flag"
	"fmt"
	"github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"go.uber.org/zap"
	"strings"
	"time"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	)

// Config for a StorageClient
type Config struct {
	Addresses                string        `yaml:"addresses,omitempty"`
	Port                     int           `yaml:"port,omitempty"`
	Keyspace                 string        `yaml:"keyspace,omitempty"`
	Consistency              string        `yaml:"consistency,omitempty"`
	ReplicationFactor        int           `yaml:"replication_factor,omitempty"`
	DisableInitialHostLookup bool          `yaml:"disable_initial_host_lookup,omitempty"`
	SSL                      bool          `yaml:"SSL,omitempty"`
	HostVerification         bool          `yaml:"host_verification,omitempty"`
	CAPath                   string        `yaml:"CA_path,omitempty"`
	Auth                     bool          `yaml:"auth,omitempty"`
	Username                 string        `yaml:"username,omitempty"`
	Password                 string        `yaml:"password,omitempty"`
	Timeout                  time.Duration `yaml:"timeout,omitempty"`
	ConnectTimeout           time.Duration `yaml:"connect_timeout,omitempty"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Addresses, "cassandra.addresses", "", "Comma-separated hostnames or IPs of Cassandra instances.")
	f.IntVar(&cfg.Port, "cassandra.port", 9042, "Port that Cassandra is running on")
	f.StringVar(&cfg.Keyspace, "cassandra.keyspace", "", "Keyspace to use in Cassandra.")
	f.StringVar(&cfg.Consistency, "cassandra.consistency", "QUORUM", "Consistency level for Cassandra.")
	f.IntVar(&cfg.ReplicationFactor, "cassandra.replication-factor", 1, "Replication factor to use in Cassandra.")
	f.BoolVar(&cfg.DisableInitialHostLookup, "cassandra.disable-initial-host-lookup", false, "Instruct the cassandra driver to not attempt to get host info from the system.peers table.")
	f.BoolVar(&cfg.SSL, "cassandra.ssl", false, "Use SSL when connecting to cassandra instances.")
	f.BoolVar(&cfg.HostVerification, "cassandra.host-verification", true, "Require SSL certificate validation.")
	f.StringVar(&cfg.CAPath, "cassandra.ca-path", "", "Path to certificate file to verify the peer.")
	f.BoolVar(&cfg.Auth, "cassandra.auth", false, "Enable password authentication when connecting to cassandra.")
	f.StringVar(&cfg.Username, "cassandra.username", "", "Username to use when connecting to cassandra.")
	f.StringVar(&cfg.Password, "cassandra.password", "", "Password to use when connecting to cassandra.")
	f.DurationVar(&cfg.Timeout, "cassandra.timeout", 600*time.Millisecond, "Timeout when connecting to cassandra.")
	f.DurationVar(&cfg.ConnectTimeout, "cassandra.connect-timeout", 600*time.Millisecond, "Initial connection timeout, used during initial dial to server.")
}

func (cfg *Config) session() (*gocql.Session, error) {
	consistency, err := gocql.ParseConsistencyWrapper(cfg.Consistency)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := cfg.createKeyspace(); err != nil {
		return nil, errors.WithStack(err)
	}

	cluster := gocql.NewCluster(strings.Split(cfg.Addresses, ",")...)
	cluster.Port = cfg.Port
	cluster.Keyspace = cfg.Keyspace
	cluster.Consistency = consistency
	cluster.BatchObserver = observer{}
	cluster.QueryObserver = observer{}
	cluster.Timeout = cfg.Timeout
	cluster.ConnectTimeout = cfg.ConnectTimeout
	cfg.setClusterConfig(cluster)

	return cluster.CreateSession()
}

// apply config settings to a cassandra ClusterConfig
func (cfg *Config) setClusterConfig(cluster *gocql.ClusterConfig) {
	cluster.DisableInitialHostLookup = cfg.DisableInitialHostLookup

	if cfg.SSL {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 cfg.CAPath,
			EnableHostVerification: cfg.HostVerification,
		}
	}
	if cfg.Auth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}
}

// createKeyspace will create the desired keyspace if it doesn't exist.
func (cfg *Config) createKeyspace() error {
	cluster := gocql.NewCluster(strings.Split(cfg.Addresses, ",")...)
	cluster.Port = cfg.Port
	cluster.Keyspace = "system"
	cluster.Timeout = 20 * time.Second
	cluster.ConnectTimeout = 20 * time.Second

	cfg.setClusterConfig(cluster)

	session, err := cluster.CreateSession()
	if err != nil {
		return errors.WithStack(err)
	}
	defer session.Close()

	err = session.Query(fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS %s
		 WITH replication = {
			 'class' : 'SimpleStrategy',
			 'replication_factor' : %d
		 }`,
		cfg.Keyspace, cfg.ReplicationFactor)).Exec()
	return errors.WithStack(err)
}

// NewStorageClient returns a new StorageClient.
func NewStorageClient(cfg Config, schemaCfg chunk.SchemaConfig) (*server, error) {
	session, err := cfg.session()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	logger,_ := zap.NewProduction()
	client := &server{
		Cfg:       cfg,
		SchemaCfg: schemaCfg,
		Session:   session,
		Logger:    logger,
	}
	return client, nil
}

// Stop implement chunk.IndexClient.
func (c *server) Stop(context.Context, *grpc.Nothing) (*grpc.Nothing, error) {
	c.Session.Close()
	return nil, nil
}

// Cassandra batching isn't really useful in this case, its more to do multiple
// atomic writes.  Therefore we just do a bunch of writes in parallel.
type writeBatch struct {
	entries []chunk.IndexEntry
}

func (b *writeBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	b.entries = append(b.entries, chunk.IndexEntry{
		TableName:  tableName,
		HashValue:  hashValue,
		RangeValue: rangeValue,
		Value:      value,
	})
}

// readBatch represents a batch of rows read from Cassandra.
type readBatch struct {
	consumed   bool
	rangeValue []byte
	value      []byte
}

func (r *readBatch) Iterator() chunk.ReadBatchIterator {
	return &readBatchIter{
		readBatch: r,
	}
}

type readBatchIter struct {
	consumed bool
	*readBatch
}

func (b *readBatchIter) Next() bool {
	if b.consumed {
		return false
	}
	b.consumed = true
	return true
}

func (b *readBatchIter) RangeValue() []byte {
	return b.rangeValue
}

func (b *readBatchIter) Value() []byte {
	return b.value
}

// PutChunks implements chunk.ObjectClient.
func (c *server) PutChunks(ctx context.Context, chunk *grpc.Chunk) (*grpc.Nothing, error) {
		// Must provide a range key, even though its not useds - hence 0x00.
	    c.Logger.Info("performing put chunks.")
		q := c.Session.Query(fmt.Sprintf("INSERT INTO %s (hash, range, value) VALUES (?, 0x00, ?)",
			chunk.TableName), chunk.Key, chunk.Buf)
		if err := q.WithContext(ctx).Exec(); err != nil {
			c.Logger.Error("failed to put chunks %s", zap.Error(err))
			return &grpc.Nothing{}, errors.WithStack(err)
		}

	return &grpc.Nothing{}, nil
}

func (c *server) DeleteChunks(ctx context.Context, chunkID *grpc.ChunkID) (*grpc.Nothing, error) {
	return &grpc.Nothing{}, chunk.ErrNotSupported
}

// GetChunks implements chunk.ObjectClient.
func (c *server) GetChunks(ctx context.Context, input *grpc.Chunks) (*grpc.Chunks, error) {
	c.Logger.Info("performing get chunks.")
	chunkInfo := chunk.Chunk{}
	var chunksInfo []chunk.Chunk
	chunkInfo.Metric = labels.Labels{}
	for _, chunkData := range input.ChunkInfo {
		chunkInfo.From = model.TimeFromUnix(chunkData.From)
		chunkInfo.Through = model.TimeFromUnix(chunkData.Through)
		chunkInfo.Fingerprint = model.Fingerprint(chunkData.FingerPrint)
		chunkInfo.Encoding = encoding.Encoding(chunkData.Encoding[0])
		chunkInfo.Checksum = chunkData.Checksum
		metric := &labels.Label{}
		var metrics labels.Labels
		for _, metricLabels := range chunkData.Metric{
			metric.Name = metricLabels.Name
			metric.Value = metricLabels.Value
			metrics = append(metrics, *metric)
		}
		chunkInfo.Metric = metrics
		chunksInfo = append(chunksInfo, chunkInfo)
	}

	grpcChunks := &grpc.Chunks{}
	grpcChunk := &grpc.ChunkInfo{}
	chunks, err := util.GetParallelChunks(ctx, chunksInfo, c.getChunk)
	if err != nil {
		c.Logger.Error("failed to get parallel chunks %s", zap.Error(err))
		return grpcChunks, errors.WithStack(err)
	}


	for _, chunkData := range chunks {
		grpcChunk.From = int64(chunkData.From)
		grpcChunk.Checksum = chunkData.Checksum
		grpcChunk.Encoding = []byte(chunkData.Encoding.String())
		grpcChunk.Through = int64(chunkData.Through)
		grpcChunk.FingerPrint = int64(chunkData.Fingerprint)
		grpcChunk.ChecksumSet = chunkData.ChecksumSet
		grpcChunk.UserID = chunkData.UserID
		metric := &grpc.Label{}
		var metrics []*grpc.Label
		for _, metricLabels := range chunkData.Metric {
			metric.Name = metricLabels.Name
			metric.Value = metricLabels.Value
			metrics = append(metrics, metric)
		}
		grpcChunk.Metric = metrics
	}
	grpcChunks.ChunkInfo = append(grpcChunks.ChunkInfo, grpcChunk)
	return grpcChunks, err
}

func (s *server) getChunk(ctx context.Context, decodeContext *chunk.DecodeContext, input chunk.Chunk) (chunk.Chunk, error) {
	tableName, err := s.SchemaCfg.ChunkTableFor(input.From)
	if err != nil {
		s.Logger.Error("failed to get get chunk %s", zap.Error(err))
		return input, err
	}

	var buf []byte
	if err := s.Session.Query(fmt.Sprintf("SELECT value FROM %s WHERE hash = ?", tableName), input.ExternalKey()).
		WithContext(ctx).Scan(&buf); err != nil {
		return input, errors.WithStack(err)
	}
	err = input.Decode(decodeContext, buf)
	return input, err
}