import { unlink, writeFile } from 'fs/promises'

import log from 'logjs'
import { upload } from 'googlejs/storage'

import { sql } from '../db/index.mjs'
import { makeCSV } from './util.mjs'

const debug = log
  .prefix('export:stocks:')
  .colour()
  .level(2)

const STOCKS_URI = 'gs://finance-readersludlow/stocks.csv'
const TEMPFILE = '/tmp/stocks.csv'

export default async function exportStocks () {
  const data = selectStockSheet()
    .map(stockToRow)
    .map(makeCSV)
    .join('')

  await writeFile(TEMPFILE, data)
  await upload(TEMPFILE, STOCKS_URI, { acl: 'public' })
  await unlink(TEMPFILE)
  debug('stocks written to %s', STOCKS_URI)
}

function stockToRow (row) {
  const { ticker, incomeType, name, price, dividend, notes } = row
  return [
    ticker,
    incomeType || '',
    name || '',
    price || 0,
    dividend || 0,
    notes || ''
  ]
}

const selectStockSheet = sql(`
  SELECT  ticker, incomeType, name, price, dividend, notes
  FROM    stock_view
  ORDER BY ticker
`).all
