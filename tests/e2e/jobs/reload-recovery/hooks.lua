-- Fixture for the hot-reload / cooperative-cancel / durable-recovery tests.
--
-- Everything is deterministic: override_fetch never touches the real network
-- for the data path, and each fetched page yields one item whose title is the
-- URL. The per-URL sleep gate lets a test park the worker in-flight so a
-- reload or timeout lands while work is still running.
_G.log = {}
_G.cycles = 0
_G.fetched = {}
_G.stored = {}
_G.pushed_extra = false
_G.fetch_sleep_ms = 0
_G.slow_url = nil

local function item_for(url)
    local title = url:gsub("^.*/", "")
    return { fields = { title = title } }
end

function on_start(queue)
    _G.cycles = _G.cycles + 1
    -- queue_push is only valid once the queue cycle is active, so the extra
    -- paginated URL is pushed from the first override_fetch below instead.
    return queue.urls
end

function override_fetch(request)
    -- Exactly one mid-run push per process: exercises the durable enqueue
    -- path that queue_push takes before the in-memory send.
    if not _G.pushed_extra then
        _G.pushed_extra = true
        queue_push("http://127.0.0.1:1/page/pushed-1")
    end
    if _G.slow_url and request.url == _G.slow_url then
        sleep(_G.fetch_sleep_ms)
    end
    table.insert(_G.fetched, request.url)
    return {
        status = 200,
        url = request.url,
        headers = {},
        body = "<html></html>",
    }
end

function override_extract(response)
    return { item_for(response.url) }
end

function before_store(items)
    for _, item in ipairs(items) do
        table.insert(_G.stored, item.title)
    end
    return items
end

function on_finished(queue)
    table.insert(
        _G.log,
        "on_finished:"
            .. queue.processed
            .. ":"
            .. queue.failed
            .. ":"
            .. queue.pushed
            .. ":"
            .. queue.duplicates
    )
end
