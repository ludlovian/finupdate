import log from 'logjs'

import { getSheetData } from '../sheets.mjs'
import { sql } from '../db/index.mjs'
import { importDecimal, expandDecimal } from './util.mjs'

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

const insertDividends = sql.transaction(divs => {
  for (let div of divs) {
    div = expandDecimal(div, 'dividend')
    insertDividend(div)
  }
})

const insertDividend = sql(`
  INSERT INTO stock_dividend_v (ticker, dividend, dividendFactor, source)
    VALUES ($ticker, $dividend, $dividendFactor, $source)
`)

const insertPositions = sql.transaction(posns => {
  clearPositions()
  for (let pos of posns) {
    pos = expandDecimal(pos, 'qty')
    pos.qty = pos.qty || 0
    pos.qtyFactor = pos.qtyFactor || 1
    insertPosition(pos)
  }
})

const clearPositions = sql(`
  UPDATE position
    SET   (qty, qtyFactor, source, updated) =
            (0, 1, NULL, datetime('now'))
    WHERE qty != 0
`)

const insertPosition = sql(`
  INSERT INTO position_v (person, account, ticker, qty, qtyFactor, source)
    VALUES ($person, $account, $ticker, $qty, $qtyFactor, $source)
`)
