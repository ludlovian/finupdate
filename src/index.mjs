import mri from 'mri'
import main from './main.mjs'

const version = '__VERSION__'
const opts = mri(process.argv.slice(2), {
  alias: {
    help: 'h',
    version: 'v'
  }
})
if (opts.version) {
  console.log('finupdate %s', version)
} else if (opts.help) {
  console.log(
    '\n  Usage\n\n' +
      '    finupdate [options] <commands>\n\n' +
      '  Commands\n' +
      '    import-portfolio  Read in the portfolio spreadsheet\n' +
      '    import-stocks     Read in the stocks spreadsheet\n' +
      '    import-trades     Read in the trades spreadsheet\n' +
      '    fetch-prices      Fetch stock prices\n' +
      '    export-positions  Write out the positions data\n' +
      '    export-trades     Write out the trades data\n' +
      '    export-stocks     Write out the stocks data\n' +
      '    import            Perform all the imports\n' +
      '    fetch             Perform all fetches\n' +
      '    export            Perform all the exports\n' +
      '\n' +
      '  Options\n' +
      '    -v, --version     Display current version\n' +
      '    -h, --help        Displays this message\n'
  )
} else {
  main(opts)
}
