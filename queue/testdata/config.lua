local queue = require('queue')
rawset(_G, 'queue', queue)

-- Do not set listen for now so connector won't be
-- able to send requests until everything is configured.
box.cfg{
    work_dir = os.getenv("TEST_TNT_WORK_DIR"),
}

box.once("init", function()
    box.schema.user.create('test', {password = 'test'})
    box.schema.func.create('queue.tube.test_queue:touch')
    box.schema.func.create('queue.tube.test_queue:ack')
    box.schema.func.create('queue.tube.test_queue:put')
    box.schema.func.create('queue.tube.test_queue:drop')
    box.schema.func.create('queue.tube.test_queue:peek')
    box.schema.func.create('queue.tube.test_queue:kick')
    box.schema.func.create('queue.tube.test_queue:take')
    box.schema.func.create('queue.tube.test_queue:delete')
    box.schema.func.create('queue.tube.test_queue:release')
    box.schema.func.create('queue.tube.test_queue:release_all')
    box.schema.func.create('queue.tube.test_queue:bury')
    box.schema.func.create('queue.identify')
    box.schema.func.create('queue.state')
    box.schema.func.create('queue.statistics')
    box.schema.user.grant('test', 'create,read,write,drop', 'space')
    box.schema.user.grant('test', 'read, write', 'space', '_queue_session_ids')
    box.schema.user.grant('test', 'execute', 'universe')
    box.schema.user.grant('test', 'read,write', 'space', '_queue')
    box.schema.user.grant('test', 'read,write', 'space', '_schema')
    box.schema.user.grant('test', 'read,write', 'space', '_space_sequence')
    box.schema.user.grant('test', 'read,write', 'space', '_space')
    box.schema.user.grant('test', 'read,write', 'space', '_index')
    box.schema.user.grant('test', 'read,write', 'space', '_priv')
    if box.space._trigger ~= nil then
        box.schema.user.grant('test', 'read', 'space', '_trigger')
    end
    if box.space._fk_constraint ~= nil then
        box.schema.user.grant('test', 'read', 'space', '_fk_constraint')
    end
    if box.space._ck_constraint ~= nil then
        box.schema.user.grant('test', 'read', 'space', '_ck_constraint')
    end
    if box.space._func_index ~= nil then
        box.schema.user.grant('test', 'read', 'space', '_func_index')
    end
end)

-- Set listen only when every other thing is configured.
box.cfg{
    listen = os.getenv("TEST_TNT_LISTEN"),
}

require('console').start()