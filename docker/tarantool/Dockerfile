ARG VERSION=2.6.0
FROM tarantool/tarantool:${VERSION}

COPY users.lua /etc/tarantool/instances.enabled/users.lua
CMD ["tarantool", "/etc/tarantool/instances.enabled/users.lua"]