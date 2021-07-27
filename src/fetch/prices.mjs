import { get } from 'https'

import log from 'logjs'

import { sql } from '../db/index.mjs'
import { convertDecimal } from '../import/util.mjs'

const debug = log
  .prefix('fetch:')
  .colour()
  .level(2)

export default async function fetchPrices () {
  const tickers = selectActiveStocks().map(s => s + '.L')
  const url = (
    'https://query1.finance.yahoo.com/v7/finance/quote?symbols=' +
    tickers.join(',')
  )
  const data = await fetchData(url)
  if (!data.quoteResponse || data.quoteResponse.error) {
    throw Object.assign(new Error('Bad response'), { data })
  }

  insertYahooData(data.quoteResponse.result)

  log('data gathered from yahoo')
}

async function fetchData (url) {
  const res = await getResponse(url)

  let data = ''
  res.setEncoding('utf8')

  for await (const chunk of res) {
    data += chunk
  }

  return JSON.parse(data)

  function getResponse (url) {
    return new Promise((resolve, reject) => {
      const req = get(url, res => {
        const { statusCode } = res
        if (statusCode >= 400) {
          const { statusMessage, headers } = res
          const e = new Error(statusMessage)
          e.statusMessage = statusMessage
          e.statusCode = statusCode
          e.headers = e.headers
          e.url = e.url
          return reject(e)
        }
        resolve(res)
      })
      req.on('error', reject)
    })
  }
}

const selectActiveStocks = sql(`
  SELECT ticker FROM stocks_in_use_v
`).pluck().all

const clearMarketData = sql(`
  DELETE from yahoo_data
  WHERE  stockId IN (
    SELECT stockId
    FROM   stocks_in_use_v
  );
`)

const insertValue = sql(`
  INSERT INTO yahoo_data_v
    (ticker, kind, value)
  VALUES
    ($ticker, $kind, $value)
`)

const insertYahooData = sql.transaction(stocks => {
  clearMarketData()
  for (const stock of stocks) {
    ticker = stock.symbol.replace(/\.L/$, '')
    for (const [kind, value] of Object.entries(stock)) {
      if (kind !== 'symbol') insertValue({ ticker, kind, value })
    }
  }
})
