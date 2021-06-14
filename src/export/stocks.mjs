import { unlink, writeFile } from 'fs/promises'

import log from 'logjs'
import { upload } from 'googlejs/storage'

import { getStocks } from '../db.mjs'
import { exportDecimal, makeCSV } from './util.mjs'

const debug = log
  .prefix('export:stocks:')
  .colour()
  .level(2)

const STOCKS_URI = 'gs://finance-readersludlow/stocks.csv'
const TEMPFILE = '/tmp/stocks.csv'

export default async function exportStocks () {
  const data = getStocks()
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
    incomeType,
    name,
    exportDecimal(price),
    exportDecimal(dividend),
    notes
  ]
}
