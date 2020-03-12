# gRPC based Cassandra Server for Cortex 

./cortex-cassandra-store --config.file="grpc-cassandra.yaml"

Save below cassandra configuration to ```grpc-cassandra.yaml``` file.

```yaml
cfg:
  addresses: localhost # configure cassandra addresses here.
  keyspace: cortex1   # configure desired keyspace here.
  username: cassandras
  password: cassandra
  auth: true

schema_cfg:
  configs:
  - from: 2019-07-29
    store: grpc
    object_store: grpc
    schema: v10
    index:
      prefix: index_
      period: 168h
    chunks:
      prefix: chunk_
      period: 168h
```

Steps to run gRPC Cassandra server:

Run Cassandra:
```yaml
docker run -d --name cassandra --rm -p 9042:9042 cassandra:3.11
```
Run Cortex gRPC server for cassandra:

```yaml
cd bin
./cortex-cassandra-store --config.file=grpc-cassandra.yaml
```

Now run Cortex and configure the gRPC server address in Cortex ```--config.file``` as mentioned below

```yaml
# Configuration for running Cortex in single-process mode.
# This should not be used in production.  It is only for getting started
# and development.

# Disable the requirement that every request to Cortex has a
# X-Scope-OrgID header. `fake` will be substituted in instead.
auth_enabled: false

server:
  http_listen_port: 9009

  # Configure the server to allow messages up to 100MB.
  grpc_server_max_recv_msg_size: 104857600
  grpc_server_max_send_msg_size: 104857600
  grpc_server_max_concurrent_streams: 1000

distributor:
  shard_by_all_labels: true
  pool:
    health_check_ingesters: true

ingester_client:
  grpc_client_config:
    # Configure the client to allow messages up to 100MB.
    max_recv_msg_size: 104857600
    max_send_msg_size: 104857600
    use_gzip_compression: true

ingester:
  lifecycler:
    # The address to advertise for this ingester. Will be autodiscovered by
    # looking up address on eth0 or en0; can be specified if this fails.
    address: 127.0.0.1

    # We want to start immediately and flush on shutdown.
    join_after: 0
    claim_on_rollout: false
    final_sleep: 0s
    num_tokens: 512

    # Use an in memory ring store, so we don't need to launch a Consul.
    ring:
      kvstore:
        store: inmemory
      replication_factor: 1

# Use gRPC based storage backend -for both index store and chunks store.
schema:
  configs:
  - from: 2019-07-29
    store: grpc
    object_store: grpc
    schema: v10
    index:
      prefix: index_
      period: 168h
    chunks:
      prefix: chunk_
      period: 168h

storage:
  grpc: 
    address: localhost:6666

```