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

## Mappings

Replicator can map MySQL tables to one or more Tarantool spaces. 
Each mapping item contains the names of a database and a table, 
a list of replicated columns including primary keys, a space name.

If no primary keys are given, replicator reads them from MySQL table info automatically.
