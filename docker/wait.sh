#!/bin/bash

set -e

__workdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
__rootdir=$(dirname "${__workdir}")

cd "${__rootdir}"

while ! docker-compose exec mysql mysql --user=root --password=root_pwd -e "status" &> /dev/null ; do
    echo "Waiting for MySQL connection..."
    sleep 1
done
