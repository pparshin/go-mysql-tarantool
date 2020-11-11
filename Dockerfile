FROM golang:1.14.11 as builder

RUN apt-get update && apt-get install -y mariadb-client

WORKDIR /replicator
COPY go.mod go.sum ./

RUN go mod download

COPY . ./
RUN make build


FROM golang:1.14.11

WORKDIR /usr/bin
COPY --from=builder /replicator/bin/mysql-tarantool-replicator ./
COPY --from=builder /usr/bin/mysqldump /usr/bin/mysqldump

RUN chmod +x ./mysql-tarantool-replicator
COPY config/replicator.conf.yml /etc/mysql-tarantool/conf.yml

ENTRYPOINT ["/usr/bin/mysql-tarantool-replicator"]
CMD ["-config", "/etc/mysql-tarantool/conf.yml"]
