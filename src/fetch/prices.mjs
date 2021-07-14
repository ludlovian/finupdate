import log from 'logjs'

import { sql } from '../db/index.mjs'
import { expandDecimal } from '../import/util.mjs'
import { fetchIndex, fetchSector, fetchPrice } from './lse.mjs'

const debug = log
  .prefix('fetch:')
  .colour()
  .level(2)

// first try to load prices via collections - indices and sectors
const attempts = [
  ['ftse-all-share', fetchIndex],
  ['ftse-aim-all-share', fetchIndex],
  ['closed-end-investments', fetchSector]
]

export default async function fetchPrices () {
  const needed = new Set(selectActiveStocks())
  const updates = []
  for await (const item of getPrices(needed)) {
    if (item.price) updates.push(item)
  }

  insertPrices(updates)
}

async function * getPrices (tickers) {
  const needed = new Set(tickers)
  const isNeeded = ({ ticker }) => needed.delete(ticker)

  for (const [name, fetchFunc] of attempts) {
    let n = 0
    for await (const price of fetchFunc(name)) {
      if (!isNeeded(price)) continue
      n++
      yield price
    }
    debug('%d prices from %s', n, name)

    if (!needed.size) return
  }

  // now pick up the remaining ones
  for (const ticker of needed) {
    yield await fetchPrice(ticker)
  }
  debug('%d prices individually: %s', needed.size, [...needed].join(', '))
}

const selectActiveStocks = sql(`
  SELECT ticker FROM stock
  WHERE stockId IN (
    SELECT stockId FROM stock_dividend
    UNION
    SELECT stockId FROM position
    WHERE qty != 0
  )
`).pluck().all

const clearOldPrices = sql(`
  DELETE FROM stock_price
    WHERE updated < datetime('now', '-' || $days || ' days');
`)

const insertPrice = sql(`
  INSERT INTO stock_price_v
    (ticker, name, price, priceFactor, source)
  VALUES
    ($ticker, $name, $price, $priceFactor, $source)
`)

const insertPrices = sql.transaction(prices => {
  for (const price of prices) {
    insertPrice(expandDecimal(price, 'price'))
  }
  clearOldPrices({ days: 7 })
})
