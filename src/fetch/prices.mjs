import log from 'logjs'
import teme from 'teme'

import { get, put } from '../db.mjs'
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

export default async function fetchPrices (opts) {
  const activeStocks = await get('/stock/active', opts)
  const allStocks = await get('/stock', opts)

  const needed = new Set(activeStocks.map(s => s.ticker))
  const notNeeded = new Set(
    allStocks.map(s => s.ticker).filter(t => !needed.has(t))
  )

  const updates = []
  for await (const item of getPrices(needed)) {
    const stock = activeStocks.find(s => s.ticker === item.ticker)
    updates.push({
      ...item,
      name: stock.name || item.name
    })
  }

  for (const ticker of notNeeded) {
    updates.push({
      ticker,
      price: undefined,
      priceSource: undefined,
      priceUpdated: undefined
    })
  }

  await put('/stock', updates, opts)
}

async function * getPrices (tickers) {
  const needed = new Set(tickers)
  const isNeeded = ({ ticker }) => needed.delete(ticker)

  for (const [name, fetchFunc] of attempts) {
    let n = 0
    const prices = teme(fetchFunc(name))
      .filter(isNeeded)
      .each(() => n++)
    yield * prices
    debug('%d prices from %s', n, name)

    if (!needed.size) return
  }

  // now pick up the remaining ones
  for (const ticker of needed) {
    yield await fetchPrice(ticker)
  }
  debug('%d prices individually: %s', needed.size, [...needed].join(', '))
}
