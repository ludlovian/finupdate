import log from 'logjs'
import sortBy from 'sortby'
import decimal from 'decimal'

import { getSheetData } from '../sheets.mjs'
import { insertTrades } from '../db/index.mjs'
import { importDate, importDecimal } from './util.mjs'

const debug = log
  .prefix('import:trades:')
  .colour()
  .level(2)

const source = {
  name: 'Trades',
  range: 'Trades!A2:F'
}

export default async function importTrades () {
  const rangeData = await getSheetData(source.name, source.range)

  const updates = []
  const groups = getTradeGroups(rangeData)

  for (const group of groups) {
    if (!group.length) continue
    let seq = 0
    for (const trade of group) {
      trade.seq = ++seq
      trade.source = 'sheets:trades'
      updates.push(trade)
    }
  }

  insertTrades('Dealing', updates)
  debug('Updated %d positions with %d trades', groups.length, updates.length)
}

function getTradeGroups (rows) {
  const rawTrades = readTrades(rows)
  const sortedTrades = sortTrades(rawTrades)
  const groups = groupTrades(sortedTrades)
  addCosts(groups)

  return groups
}

function readTrades (rows) {
  const account = 'Dealing'
  let person
  let ticker
  const rowToObject = row => {
    const [person_, ticker_, date, qty, cost, notes] = row
    return {
      person: (person = person_ || person),
      account,
      ticker: (ticker = ticker_ || ticker),
      date: importDate(date),
      qty: importDecimal(qty, 0),
      cost: importDecimal(cost, 2),
      notes
    }
  }

  const validTrade = t => t.person && t.ticker && t.date && (t.qty || t.cost)

  return rows.map(rowToObject).filter(validTrade)
}

function sortTrades (trades) {
  return trades.sort(
    sortBy('person')
      .thenBy('account')
      .thenBy('ticker')
      .thenBy('date')
  )
}

function groupTrades (trades) {
  const key = t => `${t.person}_${t.account}_${t.ticker}`
  const groups = []
  let prev
  let group
  for (const trade of trades) {
    const k = key(trade)
    if (k !== prev) {
      prev = k
      group = []
      groups.push(group)
    }
    group.push(trade)
  }
  return groups
}

function addCosts (groups) {
  groups.forEach(group => group.forEach(buildPosition()))
}

function buildPosition () {
  const pos = { qty: decimal(0n), cost: decimal('0.00') }
  return trade => {
    const { qty, cost } = trade
    if (qty && cost && qty.cmp(0n) > 0) {
      // buy
      pos.qty = pos.qty.add(qty)
      pos.cost = pos.cost.add(cost)
    } else if (qty && cost && qty.cmp(0n) < 0) {
      const prev = { ...pos }
      const proceeds = cost.abs()
      pos.qty = pos.qty.add(qty)
      const remain = prev.qty.eq(0n)
        ? decimal(0n)
        : pos.qty.withPrecision(9).div(prev.qty)
      pos.cost = prev.cost.mul(remain)
      trade.cost = prev.cost.sub(pos.cost).neg()
      trade.gain = proceeds.sub(trade.cost.abs())
    } else if (qty) {
      pos.qty = pos.qty.add(qty)
    } else if (cost) {
      pos.cost = pos.cost.add(cost)
    }
  }
}
