![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/pparshin/go-mysql-tarantool?sort=semver&style=for-the-badge)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/pparshin/go-mysql-tarantool/CI?style=for-the-badge)
![Coverage Status](https://img.shields.io/codecov/c/github/pparshin/go-mysql-tarantool?style=for-the-badge)


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

### Force cast MySQL value

Replicator can cast the value from MySQL to the required type 
if your Tarantool schema does not comply with the MySQL schema.
For example, MySQL column stores `bigint(20)` values, but Tarantool
expects `unsigned`. 
Without explicit casting you get an error, e.g.:
> Tuple field 1 type does not match one required by operation: expected unsigned

Supported types to cast to:
* `unsigned`: try to cast any number to unsigned value.

To enable this feature specify which column should be casted:

```yaml
...
  mappings:
    - source:
        schema: 'city'
        table: 'users'
        columns:
          - client_id
      dest:
        space: 'users'
        cast:
          client_id: 'unsigned'
```

## Docker image

Image available at [Docker Hub](https://hub.docker.com/r/pparshin/go-mysql-tarantool).

How to build:

```bash
docker build -t mysql-tarantool-replicator:latest .
```

How to use:

```bash
docker run -it --rm -v /my/custom/conf.yml:/etc/mysql-tarantool/conf.yml mysql-tarantool-replicator
```

## Metrics

Replicator exposes several debug endpoints:

* `/metrics` - runtime and app metrics in Prometheus format,
* `/health` - health check.

Health check returns status `503 Service Unavailable` if replicator is not running, dumping 
data or replication lag greater than `app.health.seconds_behind_master` config value.
