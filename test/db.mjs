import { test } from 'uvu'
import * as assert from 'uvu/assert'

import { execFileSync as exec } from 'child_process'

import SQLite from 'better-sqlite3'
import decimal from 'decimal'

import * as dbCmd from '../src/db/index.mjs'

const DB_DIR = 'test/assets'

const DELETE_ALL = `
  delete from trade;
  delete from position;
  delete from stock_price;
  delete from stock_dividend;
  delete from stock;
`

test.before(ctx => {
  exec('rm', ['-rf', DB_DIR])
  exec('mkdir', ['-p', DB_DIR])
  const dbFile = DB_DIR + '/test.db'
  process.env.DB = dbFile
  try {
    dbCmd.open()
  } catch (e) {
    console.error(e)
    process.exit(1)
  }
  ctx.db = new SQLite(dbFile)
})

test.after(() => {
  exec('rm', ['-rf', DB_DIR])
})

test('creation', ({ db }) => {
  const res = db.pragma('user_version', { simple: true })
  assert.is(res, 2)
})

test('insert & update stock', ({ db }) => {
  db.exec(DELETE_ALL)

  const args = [
    { ticker: 'foo', name: 'bar', incomeType: 'baz', notes: 'quux' }
  ]

  dbCmd.insertStocks(args)

  let res = db
    .prepare('select ticker, name, incomeType, notes from stock')
    .raw()
    .all()
  let exp = [['foo', 'bar', 'baz', 'quux']]
  assert.equal(res, exp)

  args[0].name = 'bar2'
  args[0].incomeType = 'baz2'
  args[0].notes = 'quux2'

  dbCmd.insertStocks(args)

  res = db
    .prepare('select ticker, name, incomeType, notes from stock')
    .raw()
    .all()
  exp = [['foo', 'bar2', 'baz2', 'quux2']]
  assert.equal(res, exp)
})

test('insert price', ({ db }) => {
  db.exec(DELETE_ALL)

  const args = [
    { ticker: 'foo', name: 'bar', price: decimal('123.45'), source: 'quux' }
  ]

  dbCmd.insertPrices(args)

  let res = db
    .prepare('select ticker, name, price from stock_view where ticker=?')
    .raw()
    .get('foo')
  assert.equal(res, ['foo', 'bar', 123.45])

  args[0].name = 'bar2'
  args[0].price = decimal('234.56')

  dbCmd.insertPrices(args)

  res = db
    .prepare('select ticker, name, price from stock_view where ticker=?')
    .raw()
    .get('foo')
  assert.equal(res, ['foo', 'bar', 234.56])
})

test('insert dividend', ({ db }) => {
  db.exec(DELETE_ALL)
  dbCmd.insertPrices([{ ticker: 'foo', price: decimal(10) }])

  dbCmd.insertDividends([
    { ticker: 'foo', dividend: decimal(1.23), source: 'bar' }
  ])

  let res = db
    .prepare('select ticker, dividend, yield from stock_view where ticker=?')
    .raw()
    .get('foo')
  assert.equal(res, ['foo', 1.23, 0.123])

  dbCmd.insertDividends([
    { ticker: 'foo', dividend: decimal(1.24), source: 'bar' }
  ])

  res = db
    .prepare('select ticker, dividend, yield from stock_view where ticker=?')
    .raw()
    .get('foo')
  assert.equal(res, ['foo', 1.24, 0.124])
  dbCmd.insertDividends([{ ticker: 'foo' }])

  res = db
    .prepare('select ticker, dividend, yield from stock_view where ticker=?')
    .raw()
    .get('foo')
  assert.equal(res, ['foo', null, null])
})

test('insert position', ({ db }) => {
  db.exec(DELETE_ALL)

  dbCmd.insertPositions([
    {
      ticker: 'foo',
      person: 'AJL',
      account: 'ISA',
      qty: decimal('120'),
      source: 'foo'
    }
  ])

  const sql =
    'select qty, value, income from position_view where ticker=? and account=? and person=?'
  let res = db
    .prepare(sql)
    .raw()
    .get('foo', 'ISA', 'AJL')
  assert.equal(res, [120, null, null])

  dbCmd.insertPrices([{ ticker: 'foo', price: decimal('8.00') }])
  dbCmd.insertDividends([{ ticker: 'foo', dividend: decimal('0.25') }])

  res = db
    .prepare(sql)
    .raw()
    .get('foo', 'ISA', 'AJL')
  assert.equal(res, [120, 960, 30])
})

test('insert trade', ({ db }) => {
  db.exec(DELETE_ALL)

  dbCmd.insertTrades('ISA', [
    {
      ticker: 'foo',
      person: 'AJL',
      account: 'ISA',
      seq: 1,
      date: '2021-01-19',
      cost: decimal('12.34'),
      qty: decimal('2'),
      gain: decimal('3.45'),
      notes: 'foo',
      source: 'bar'
    }
  ])

  const sql =
    'select date, qty, cost, gain, notes from trade_view where ticker=? and account=? and person=?'
  let res = db
    .prepare(sql)
    .raw()
    .get('foo', 'ISA', 'AJL')
  assert.equal(res, ['2021-01-19', 2, 12.34, 3.45, 'foo'])

  dbCmd.insertTrades('ISA', [
    {
      ticker: 'foo',
      person: 'AJL',
      account: 'ISA',
      seq: 1,
      date: '2021-08-22',
      cost: decimal('23.45'),
      qty: decimal('3'),
      gain: decimal('4.56'),
      notes: 'foo2',
      source: 'bar'
    }
  ])
  res = db
    .prepare(sql)
    .raw()
    .get('foo', 'ISA', 'AJL')
  assert.equal(res, ['2021-08-22', 3, 23.45, 4.56, 'foo2'])

  dbCmd.insertTrades('ISA', [])
  res = db
    .prepare(sql)
    .raw()
    .get('foo', 'ISA', 'AJL')
  assert.is(res, undefined)
})

test.run()
