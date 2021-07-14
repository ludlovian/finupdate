import importStocks from './import/stocks.mjs'
import importPortfolio from './import/portfolio.mjs'
import importTrades from './import/trades.mjs'
import fetchPrices from './fetch/prices.mjs'
import exportPositions from './export/positions.mjs'
import exportTrades from './export/trades.mjs'
import exportStocks from './export/stocks.mjs'

export default async function main (opts) {
  const cmds = opts._
  while (cmds.length) {
    const cmd = cmds.shift()
    if (cmd === 'import-stocks') {
      await importStocks().catch(bail)
    } else if (cmd === 'import-portfolio') {
      await importPortfolio().catch(bail)
    } else if (cmd === 'import-trades') {
      await importTrades(opts).catch(bail)
    } else if (cmd === 'fetch-prices') {
      await fetchPrices(opts).catch(bail)
    } else if (cmd === 'export-positions') {
      await exportPositions(opts).catch(bail)
    } else if (cmd === 'export-trades') {
      await exportTrades(opts).catch(bail)
    } else if (cmd === 'export-stocks') {
      await exportStocks(opts).catch(bail)
    } else if (cmd === 'import') {
      cmds.unshift('import-stocks', 'import-portfolio', 'import-trades')
    } else if (cmd === 'fetch') {
      cmds.unshift('fetch-prices')
    } else if (cmd === 'export') {
      cmds.unshift('export-positions', 'export-trades', 'export-stocks')
    } else {
      console.error('No such command: %s', cmd)
      process.exit(1)
    }
  }
}

function bail (err) {
  console.error(err)
  process.exit(2)
}
