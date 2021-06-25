import { readdirSync, readFileSync } from 'fs'
import { join } from 'path'
import { tidy } from '../src/lib/sql.mjs'

const DIR = process.argv[2]

console.log("import SQL from '../lib/sql.mjs'")

for (const name of readdirSync(DIR)) {
  const file = join(DIR, name)
  const t = JSON.stringify(tidy(readFileSync(file, 'utf8')))
  const v = name.split('.')[0]
  console.log(`export const ${v}=SQL.from(${t});`)
}
