import log from 'logjs'

import { getSheetData } from '../sheets.mjs'
import { insertDividends, insertPositions } from '../db/index.mjs'
import { importDecimal } from './util.mjs'

const debug = log
  .prefix('import:portfolio:')
  .colour()
  .level(2)

const SOURCE = {
  name: 'Portfolio',
  range: 'Investments!A:AM'
}

const TICKER_COLUMN = 10 // column K
const ACCOUNT_COLUMN = 0 // column A
const ACCOUNT_LIST =
  'AJL,ISA;RSGG,ISA;AJL,Dealing;RSGG,Dealing;AJL,SIPP;RSGG,SIPP;RSGG,SIPP2'
const DIV_COLUMN = 26 // column AA
const accts = ACCOUNT_LIST.split(';')
  .map(code => code.split(','))
  .map(([person, account]) => ({ person, account }))

export default async function importPortfolio () {
  const rangeData = await getSheetData(SOURCE.name, SOURCE.range)

  await importDividends(rangeData)
  await importPositions(rangeData)
}

async function importDividends (rangeData) {
  const extractData = row => [row[TICKER_COLUMN], row[DIV_COLUMN]]
  const validTicker = ([ticker]) => !!ticker
  const makeObj = ([ticker, dividend]) => ({
    ticker,
    dividend: importDecimal(dividend),
    source: 'sheets:portfolio'
  })

  const data = rangeData
    .map(extractData)
    .filter(validTicker)
    .map(makeObj)

  insertDividends(data)
  debug('Updated %d dividends', data.length)
}

async function importPositions (rangeData, opts) {
  const extractRow = row => [
    row[TICKER_COLUMN],
    accts,
    row.slice(ACCOUNT_COLUMN, ACCOUNT_COLUMN + accts.length)
  ]
  const validRow = ([ticker]) => !!ticker
  const expandPositons = ([ticker, accts, qtys]) =>
    qtys.map((qty, i) => ({ ...accts[i], ticker, qty: importDecimal(qty, 0) }))
  const validPos = p => !!p.qty

  const updates = rangeData
    .map(extractRow)
    .filter(validRow)
    .map(expandPositons)
    .flat(1)
    .filter(validPos)
    .map(o => ({ ...o, source: 'sheets:portfolio' }))

  insertPositions(updates)

  debug('%d positions updated', updates.length)
}
