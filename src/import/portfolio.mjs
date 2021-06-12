import log from 'logjs'

import { getSheetData } from '../sheets.mjs'
import { get, put, del } from '../db.mjs'
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

export default async function importPortfolio (opts) {
  const rangeData = await getSheetData(SOURCE.name, SOURCE.range)

  await updateDividends(rangeData, opts)
  await updatePositions(rangeData, opts)
}

async function updateDividends (rangeData, opts) {
  const stocks = await get('/stock', opts)
  const tickers = new Set(stocks.map(s => s.ticker))
  const updates = []
  for (const item of getDividendData(rangeData)) {
    updates.push(item)
    tickers.delete(item.ticker)
  }
  const changed = updates.length
  for (const ticker of tickers) {
    updates.push({ ticker, dividend: undefined })
  }
  debug(
    'Updated %d and cleared %d dividends from portfolio sheet',
    changed,
    tickers.size
  )

  await put('/stock', updates, opts)
}

function getDividendData (rangeData) {
  const extractData = row => [row[TICKER_COLUMN], row[DIV_COLUMN]]
  const validTicker = ([ticker]) => !!ticker
  const makeObj = ([ticker, dividend]) => ({
    ticker,
    dividend: importDecimal(dividend)
  })

  return rangeData
    .map(extractData)
    .filter(validTicker)
    .map(makeObj)
}

async function updatePositions (rangeData, opts) {
  const positions = await get('/position', opts)
  const key = p => `${p.who}:${p.account}:${p.ticker}`
  const old = new Map(positions.map(p => [key(p), p]))
  const updates = []
  for (const item of getPositionData(rangeData)) {
    updates.push(item)
    old.delete(key(item))
  }
  const deletes = [...old.values()]

  if (updates.length) {
    await put('/position', updates, opts)
    debug('%d positions updated', updates.length)
  }
  if (deletes.length) {
    await del('/position', deletes, opts)
    debug('%d position(s) deleted', deletes.length)
  }
}

function * getPositionData (rangeData) {
  const accts = ACCOUNT_LIST.split(';')
    .map(code => code.split(','))
    .map(([who, account]) => ({ who, account }))

  const extractRow = row => [
    row[TICKER_COLUMN],
    accts,
    row.slice(ACCOUNT_COLUMN, ACCOUNT_COLUMN + accts.length)
  ]
  const validRow = ([ticker]) => !!ticker

  const rows = rangeData.map(extractRow).filter(validRow)

  for (const [ticker, accts, qtys] of rows) {
    yield * getPositionsFromRow(ticker, accts, qtys)
  }
}

function * getPositionsFromRow (ticker, accts, qtys) {
  const makePos = (qty, i) => ({
    ticker,
    ...accts[i],
    qty: importDecimal(qty, 0)
  })
  const validPos = x => !!x.qty

  const positions = qtys.map(makePos).filter(validPos)

  yield * positions
}
