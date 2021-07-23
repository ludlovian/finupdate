import tinydate from 'tinydate'

import decimal from 'decimal'
import { toDate } from 'googlejs/sheets'

export function importDecimal (x, prec) {
  if (x == null || x === '') return undefined
  const d = decimal(x)
  if (prec != null) return d.withPrecision(prec)
  return d
}

const plainDateString = tinydate('{YYYY}-{MM}-{DD}')

export function importDate (x) {
  return typeof x === 'number' ? plainDateString(toDate(x)) : undefined
}

export function convertDecimal (obj) {
  const r = {}
  for (const [k, v] of Object.entries(obj)) {
    if (decimal.isDecimal(v)) {
      r[k] = v.toString()
    } else {
      r[k] = v
    }
  }
  return r
}
