# MySQL - Tarantool replicator

It is a service to replicate data from MySQL into Tarantool automatically.

It uses `mysqldump` to fetch the origin data at first, then syncs data incrementally with binlog.

## Requirements

- MySQL supported version >= 5.7, MariaDB is not supported right now.
- Tarantool >= 1.10 (other versions are not tested).
- Binlog format must be set to `ROW`.
- Binlog row image must be full for MySQL.
  you may lost some field data if you update PK data in MySQL with minimal or noblob binlog row image
- `mysqldump` must exist in the same node with mysql-tarantool-replicator. 
  If not, replicator will try to sync binlog only.

### MySQL

Create or use exist user with replication grants:

```sql
GRANT PROCESS, RELOAD, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;
```

## Mappings

Replicator can map MySQL tables to one or more Tarantool spaces. 
Each mapping item contains the names of a database and a table, 
a list of replicated columns, a space name.

Replicator reads primary keys from MySQL table info and sync them automatically.
Updating primary key in MySQL causes two Tarantool requests: delete an old row and insert a new one, because
it is illegal to update primary key in Tarantool.

## Metrics

Replicator exposes several debug endpoints:

* `/metrics` - runtime and app metrics in Prometheus format,
* `/health` - health check.

Health check returns status `503 Service Unavailable` if replicator is not running, dumping 
data or replication lag greater than `app.health.seconds_behind_master` config value.