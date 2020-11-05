box.cfg { listen = 3301 }

box.once("init", function()
    box.schema.user.create('repl', { password = 'repl', if_not_exists = true })
    box.schema.user.grant('repl', 'read,write,create,execute', 'universe')

    local users_space = box.schema.space.create("users", {
        if_not_exists = true,
    })

    users_space:format({
        { name = 'id', type = 'unsigned' },
        { name = 'username', type = 'string' },
        { name = 'password', type = 'string' },
        { name = 'email', type = 'string' },
    })

    users_space:create_index('primary', {
        type = 'hash',
        if_not_exists = true,
        parts = { 'id' },
    })

    local logins_space = box.schema.space.create("logins", {
        if_not_exists = true,
    })

    logins_space:format({
        { name = 'username', type = 'string' },
        { name = 'ip', type = 'string' },
        { name = 'date', type = 'integer' },
        { name = 'attempts', type = 'unsigned' },
    })

    logins_space:create_index('primary', {
        type = 'tree',
        if_not_exists = true,
        parts = { 'username', 'ip', 'date' },
    })
end)