import log from 'logjs'

import { activeStockTickers, updatePrices } from '../db.mjs'
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
  const needed = new Set(activeStockTickers())
  const updates = []
  for await (const item of getPrices(needed)) {
    if (item.price) updates.push(item)
  }

  updatePrices(updates)
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
