export function tidy (sql) {
  return sql
    .split('\n')
    .map(line => line.trim())
    .filter(line => !line.startsWith('--'))
    .map(line => line.replaceAll(/  +/g, ' '))
    .filter(Boolean)
    .join(' ')
    .trim()
    .replaceAll(/; */g, ';')
    .replace(/;$/, '')
}

export function statement (stmt, opts = {}) {
  const { pluck, all, get, db } = opts
  function exec (...args) {
    args = args.map(cleanArg)
    if (stmt.includes(';')) return db().exec(stmt)
    let prep = prepare(stmt, db)
    if (pluck) prep = prep.pluck()
    if (all) return prep.all(...args)
    if (get) return prep.get(...args)
    return prep.run(...args)
  }
  return Object.defineProperties(exec, {
    pluck: { value: () => statement(stmt, { ...opts, pluck: true }) },
    get: { get: () => statement(stmt, { ...opts, get: true }) },
    all: { get: () => statement(stmt, { ...opts, all: true }) }
  })
}

function cleanArg (arg) {
  if (!arg || typeof arg !== 'object') return arg
  const ret = {}
  for (const k in arg) {
    const v = arg[k]
    if (v instanceof Date) {
      ret[k] = v.toISOString()
    } else if (v !== undefined) {
      ret[k] = v
    }
  }
  return ret
}

export function transaction (_fn, db) {
  let fn
  return (...args) => {
    if (!fn) fn = db().transaction(_fn)
    return fn(...args)
  }
}

const cache = new Map()
function prepare (stmt, db) {
  let p = cache.get(stmt)
  if (p) return p
  p = db().prepare(stmt)
  cache.set(stmt, p)
  return p
}
