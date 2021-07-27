import log from 'logjs'

import { sql } from '../db/index.mjs'
import { getSheetData } from '../sheets.mjs'
import { adjuster, nullIfEmpty } from './util.mjs'

const debug = log
  .prefix('import:stocks:')
  .colour()
  .level(2)

const source = {
  name: 'Stocks',
  range: 'Stocks!A:D'
}

export default async function importStocks (opts) {
  const rows = await getSheetData(source.name, source.range)

  const attrs = rows.shift()
  const validRow = row => !!row[0]
  const rowAttribs = row => attrs.map((k, ix) => [k, row[ix]])
  const validAttribs = kvs => kvs.filter(([k]) => !!k)
  const makeObject = kvs => Object.fromEntries(kvs)

  const data = rows
    .filter(validRow)
    .map(rowAttribs)
    .map(validAttribs)
    .map(makeObject)

  insertStocks(data)

  debug('Loaded %d records from stocks', data.length)
}

const insertStock = sql(`
INSERT INTO stock_v
    (ticker, name, incomeType, notes, source)
VALUES
    ($ticker, $name, $incomeType, $notes, $source)
`)

const insertStocks = sql.transaction(stocks => {
  const adj = adjuster({
    incomeType: nullIfEmpty,
    notes: nullIfEmpty,
    source: 'sheet:stocks'
  })
  for (const stock of stocks) {
    insertStock(adj(stock))
  }
})
