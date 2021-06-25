let db

const SQL = {
  from: sql => statement(tidy(sql)),
  attach: _db => (db = _db),
  transaction: fn => transaction(fn)
}

export default SQL

function statement (sql) {
  if (!Array.isArray(sql)) sql = sql.split(';')

  const s = {
    run: (...args) => exec('run', ...args),
    get: (...args) => exec('get', ...args),
    all: (...args) => exec('all', ...args),
    pluck: () => Object.assign(statement(sql), { _pluck: true }),
    raw: () => Object.assign(statement(sql), { _raw: true })
  }

  return s

  function exec (cmd, ...args) {
    const stmts = [...sql]
    let last = stmts.pop()
    for (const stmt of stmts) prepare(stmt).run(...args)
    last = prepare(last)
    if (s._pluck) last = last.pluck()
    if (s._raw) last = last.raw()
    return last[cmd](...args)
  }
}

const cache = new Map()
function prepare (sql) {
  if (!cache.has(sql)) cache.set(sql, db.prepare(sql))
  return cache.get(sql)
}

function transaction (_fn) {
  let fn
  return (...args) => {
    if (!fn) fn = db.transaction(_fn)
    return fn(...args)
  }
}

function tidyStatement (statement) {
  return statement
    .split('\n')
    .map(line => line.trim())
    .filter(line => !line.startsWith('--'))
    .map(line => line.replaceAll(/  +/g, ' '))
    .join(' ')
    .trim()
}

export function tidy (statements) {
  return statements
    .split(';')
    .map(tidyStatement)
    .filter(Boolean)
    .join(';')
}
