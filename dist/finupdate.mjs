#!/usr/bin/env node
import { homedir } from 'os';
import { join, extname } from 'path';
import SQLite from 'better-sqlite3';
import { format } from 'util';
import { get as get$1 } from 'https';
import { stat, writeFile, unlink } from 'fs/promises';
import { pipeline } from 'stream/promises';
import { createReadStream } from 'fs';
import mime from 'mime/lite.js';
import { createHash } from 'crypto';

function toArr(any) {
	return any == null ? [] : Array.isArray(any) ? any : [any];
}

function toVal(out, key, val, opts) {
	var x, old=out[key], nxt=(
		!!~opts.string.indexOf(key) ? (val == null || val === true ? '' : String(val))
		: typeof val === 'boolean' ? val
		: !!~opts.boolean.indexOf(key) ? (val === 'false' ? false : val === 'true' || (out._.push((x = +val,x * 0 === 0) ? x : val),!!val))
		: (x = +val,x * 0 === 0) ? x : val
	);
	out[key] = old == null ? nxt : (Array.isArray(old) ? old.concat(nxt) : [old, nxt]);
}

function mri (args, opts) {
	args = args || [];
	opts = opts || {};

	var k, arr, arg, name, val, out={ _:[] };
	var i=0, j=0, idx=0, len=args.length;

	const alibi = opts.alias !== void 0;
	const strict = opts.unknown !== void 0;
	const defaults = opts.default !== void 0;

	opts.alias = opts.alias || {};
	opts.string = toArr(opts.string);
	opts.boolean = toArr(opts.boolean);

	if (alibi) {
		for (k in opts.alias) {
			arr = opts.alias[k] = toArr(opts.alias[k]);
			for (i=0; i < arr.length; i++) {
				(opts.alias[arr[i]] = arr.concat(k)).splice(i, 1);
			}
		}
	}

	for (i=opts.boolean.length; i-- > 0;) {
		arr = opts.alias[opts.boolean[i]] || [];
		for (j=arr.length; j-- > 0;) opts.boolean.push(arr[j]);
	}

	for (i=opts.string.length; i-- > 0;) {
		arr = opts.alias[opts.string[i]] || [];
		for (j=arr.length; j-- > 0;) opts.string.push(arr[j]);
	}

	if (defaults) {
		for (k in opts.default) {
			name = typeof opts.default[k];
			arr = opts.alias[k] = opts.alias[k] || [];
			if (opts[name] !== void 0) {
				opts[name].push(k);
				for (i=0; i < arr.length; i++) {
					opts[name].push(arr[i]);
				}
			}
		}
	}

	const keys = strict ? Object.keys(opts.alias) : [];

	for (i=0; i < len; i++) {
		arg = args[i];

		if (arg === '--') {
			out._ = out._.concat(args.slice(++i));
			break;
		}

		for (j=0; j < arg.length; j++) {
			if (arg.charCodeAt(j) !== 45) break; // "-"
		}

		if (j === 0) {
			out._.push(arg);
		} else if (arg.substring(j, j + 3) === 'no-') {
			name = arg.substring(j + 3);
			if (strict && !~keys.indexOf(name)) {
				return opts.unknown(arg);
			}
			out[name] = false;
		} else {
			for (idx=j+1; idx < arg.length; idx++) {
				if (arg.charCodeAt(idx) === 61) break; // "="
			}

			name = arg.substring(j, idx);
			val = arg.substring(++idx) || (i+1 === len || (''+args[i+1]).charCodeAt(0) === 45 || args[++i]);
			arr = (j === 2 ? [name] : name);

			for (idx=0; idx < arr.length; idx++) {
				name = arr[idx];
				if (strict && !~keys.indexOf(name)) return opts.unknown('-'.repeat(j) + name);
				toVal(out, name, (idx + 1 < arr.length) || val, opts);
			}
		}
	}

	if (defaults) {
		for (k in opts.default) {
			if (out[k] === void 0) {
				out[k] = opts.default[k];
			}
		}
	}

	if (alibi) {
		for (k in out) {
			arr = opts.alias[k] || [];
			while (arr.length > 0) {
				out[arr.shift()] = out[k];
			}
		}
	}

	return out;
}

let db;

const SQL = {
  from: sql => statement(tidy(sql)),
  attach: _db => (db = _db),
  transaction: fn => transaction(fn)
};

function statement (sql) {
  if (!Array.isArray(sql)) sql = sql.split(';');

  const s = {
    run: (...args) => exec('run', ...args),
    get: (...args) => exec('get', ...args),
    all: (...args) => exec('all', ...args),
    pluck: () => Object.assign(statement(sql), { _pluck: true }),
    raw: () => Object.assign(statement(sql), { _raw: true })
  };

  return s

  function exec (cmd, ...args) {
    const stmts = [...sql];
    let last = stmts.pop();
    for (const stmt of stmts) prepare(stmt).run(...args);
    last = prepare(last);
    if (s._pluck) last = last.pluck();
    if (s._raw) last = last.raw();
    return last[cmd](...args)
  }
}

const cache = new Map();
function prepare (sql) {
  if (!cache.has(sql)) cache.set(sql, db.prepare(sql));
  return cache.get(sql)
}

function transaction (_fn) {
  let fn;
  return (...args) => {
    if (!fn) fn = db.transaction(_fn);
    return fn(...args)
  }
}

function tidyStatement (statement) {
  return statement
    .split('\n')
    .map(line => line.trim())
    .filter(line => !line.startsWith('--'))
    .map(line => line.replaceAll(/  +/g, ' '))
    .join(' ')
    .trim()
}

function tidy (statements) {
  return statements
    .split(';')
    .map(tidyStatement)
    .filter(Boolean)
    .join(';')
}

const clearOldDividends=SQL.from("DELETE FROM stock_dividend WHERE updated < datetime('now', '-' || ? || ' days')");
const clearOldPrices=SQL.from("DELETE FROM stock_price WHERE updated < datetime('now', '-' || ? || ' days')");
const clearPositions=SQL.from("UPDATE position SET qty = 0, qtyFactor = 1, source = NULL, updated = datetime('now') WHERE qty != 0");
const clearTrades=SQL.from("UPDATE trade SET qty = NULL, cost = NULL, gain = NULL WHERE positionId IN ( SELECT positionId FROM position WHERE accountId IN ( SELECT accountId FROM account WHERE name = ? ) )");
const ddl=SQL.from("PRAGMA journal_mode = WAL;PRAGMA foreign_keys = TRUE;CREATE VIEW IF NOT EXISTS dbversion AS SELECT 2 AS version;CREATE TABLE IF NOT EXISTS stock( stockId INTEGER PRIMARY KEY, ticker TEXT NOT NULL UNIQUE, name TEXT, incomeType TEXT, notes TEXT, source TEXT, updated TEXT NOT NULL DEFAULT (datetime('now')) );CREATE TABLE IF NOT EXISTS stock_dividend( stockId INTEGER PRIMARY KEY REFERENCES stock(stockId), dividend INTEGER NOT NULL, dividendFactor INTEGER NOT NULL DEFAULT 1, source TEXT, updated TEXT NOT NULL DEFAULT (datetime('now')) );CREATE TABLE IF NOT EXISTS stock_price( stockId INTEGER PRIMARY KEY REFERENCES stock(stockId), price INTEGER NOT NULL, priceFactor INTEGER NOT NULL, source TEXT, updated TEXT NOT NULL DEFAULT (datetime('now')) );CREATE TABLE IF NOT EXISTS account( accountId INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE );INSERT OR IGNORE INTO account (name) VALUES ('Dealing'), ('ISA'), ('SIPP'), ('SIPP2');CREATE TABLE IF NOT EXISTS person( personId INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE );INSERT OR IGNORE INTO person (name) VALUES ('AJL'), ('RSGG');CREATE TABLE IF NOT EXISTS position( positionId INTEGER PRIMARY KEY, personId INTEGER NOT NULL REFERENCES person(personId), accountId INTEGER NOT NULL REFERENCES account(accountId), stockId INTEGER NOT NULL REFERENCES stock(stockId), qty INTEGER NOT NULL, qtyFactor INTEGER NOT NULL DEFAULT 1, source TEXT, updated TEXT NOT NULL DEFAULT (datetime('now')), UNIQUE (personId, accountId, stockId) );CREATE TABLE IF NOT EXISTS trade( tradeId INTEGER PRIMARY KEY, positionId INTEGER NOT NULL REFERENCES position(positionId), seq INTEGER NOT NULL, date TEXT NOT NULL, qty INTEGER, qtyFactor INTEGER, cost INTEGER, costFactor INTEGER, gain INTEGER, gainFactor INTEGER, notes TEXT, source TEXT, updated TEXT NOT NULL DEFAULT (datetime('now')), UNIQUE (positionId, seq) );CREATE VIEW IF NOT EXISTS stock_view AS SELECT s.stockId AS stockId, s.ticker AS ticker, s.name AS name, s.incomeType AS incomeType, s.notes AS notes, CAST (d.dividend AS REAL) / CAST (d.dividendFactor AS REAL) AS dividend, CAST (p.price AS REAL) / CAST (p.priceFactor AS REAL) AS price, CAST (d.dividend * p.priceFactor AS REAL) / CAST (p.price * d.dividendFactor AS REAL) AS yield FROM stock s LEFT JOIN stock_dividend d USING (stockId) LEFT JOIN stock_price p USING (stockId) ORDER BY ticker;CREATE VIEW IF NOT EXISTS position_view AS SELECT p.positionId AS positionId, s.ticker AS ticker, a.name AS account, w.name AS person, CAST (p.qty AS REAL) / CAST (p.qtyFactor AS REAL) AS qty, round(p.qty * s.price, 2) AS value, round(p.qty * s.dividend, 2) AS income FROM position p INNER JOIN stock_view s USING (stockId) INNER JOIN account a USING (accountId) INNER JOIN person w USING (personId) WHERE qty != 0 ORDER BY ticker, account, person;CREATE VIEW IF NOT EXISTS trade_view AS SELECT t.tradeId AS tradeId, s.ticker AS ticker, a.name AS account, w.name AS person, t.date AS date, CAST (t.qty AS REAL) / CAST (t.qtyFactor AS REAL) AS qty, round(CAST (t.cost AS REAL) / CAST (t.costFactor AS REAL), 2) AS cost, round(CAST (t.gain AS REAL) / CAST (t.gainFactor AS REAL), 2) AS gain, t.notes AS notes FROM trade t, position p, account a, person w, stock s WHERE t.positionId = p.positionId AND p.accountId = a.accountId AND p.personId = w.personId AND p.stockId = s.stockId ORDER BY ticker, account, person, t.seq");
const deleteTrades=SQL.from("DELETE FROM trade WHERE qty IS NULL AND cost IS NULL AND gain IS NULL");
const insertDividend=SQL.from("INSERT OR IGNORE INTO stock (ticker) VALUES ($ticker);INSERT INTO stock_dividend ( stockId, dividend, dividendFactor, source ) SELECT stockId, $dividend, $dividendFactor, $source FROM stock WHERE ticker = $ticker AND $dividend IS NOT NULL ON CONFLICT DO UPDATE SET dividend = excluded.dividend, dividendFactor = excluded.dividendFactor, source = excluded.source;DELETE FROM stock_dividend WHERE stockId IN ( SELECT stockId FROM stock WHERE ticker = $ticker) AND $dividend IS NULL");
const insertPosition=SQL.from("INSERT OR IGNORE INTO stock (ticker) VALUES ($ticker);INSERT OR IGNORE INTO person (name) VALUES ($person);INSERT OR IGNORE INTO account (name) VALUES ($account);INSERT INTO position ( personId, accountId, stockId, qty, qtyFactor, source ) SELECT p.personId, a.accountId, s.stockId, $qty, $qtyFactor, $source FROM person p, account a, stock s WHERE p.name = $person AND a.name = $account AND s.ticker = $ticker  ON CONFLICT DO UPDATE SET qty = excluded.qty, qtyFactor = excluded.qtyFactor, source = excluded.source, updated = excluded.updated");
const insertPrice=SQL.from("INSERT OR IGNORE INTO stock(ticker) VALUES ($ticker);UPDATE stock SET name = $name WHERE ticker = $ticker AND name IS NULL;INSERT INTO stock_price ( stockId, price, priceFactor, source ) SELECT stockId, $price, $priceFactor, $source FROM stock WHERE ticker = $ticker ON CONFLICT DO UPDATE SET price = excluded.price, priceFactor = excluded.priceFactor, source = excluded.source");
const insertStock=SQL.from("INSERT INTO stock (ticker, name, incomeType, notes, source) VALUES ($ticker, $name, $incomeType, $notes, $source) ON CONFLICT DO UPDATE SET name = excluded.name, incomeType = excluded.incomeType, notes = excluded.notes, source = excluded.source, updated = excluded.updated");
const insertTrade=SQL.from("INSERT OR IGNORE INTO stock (ticker) VALUES ($ticker);INSERT OR IGNORE INTO person (name) VALUES ($person);INSERT OR IGNORE INTO account (name) VALUES ($account);INSERT OR IGNORE INTO position ( personId, accountId, stockId, qty, qtyFactor ) SELECT w.personId, a.accountId, s.stockId, 0, 1 FROM person w, account a, stock s WHERE w.name = $person AND a.name = $account AND s.ticker = $ticker;INSERT INTO trade ( positionId, seq, date, qty, qtyFactor, cost, costFactor, gain, gainFactor, notes, source ) SELECT p.positionId, $seq, $date, $qty, $qtyFactor, $cost, $costFactor, $gain, $gainFactor, $notes, $source FROM position p INNER JOIN person w USING (personId) INNER JOIN account a USING (accountId) INNER JOIN stock s USING (stockId) WHERE s.ticker = $ticker AND a.name = $account AND w.name = $person  ON CONFLICT DO UPDATE SET date = excluded.date, qty = excluded.qty, qtyFactor = excluded.qtyFactor, cost = excluded.cost, costFactor = excluded.costFactor, gain = excluded.gain, gainFactor = excluded.gainFactor, notes = excluded.notes, source = excluded.source, updated = excluded.updated");
const selectActiveStocks$1=SQL.from("SELECT ticker from stock WHERE stockId IN ( SELECT stockId FROM stock_dividend UNION SELECT stockId FROM position WHERE qty != 0 )");
const selectPositionSheet$1=SQL.from("SELECT p.ticker AS ticker, p.person AS person, p.account AS account, p.qty AS qty, p.value AS value, p.income AS income, s.price AS price, s.dividend AS dividend, s.yield AS yield  FROM position_view p, stock_view s  WHERE p.ticker = s.ticker  ORDER BY ticker, person, account");
const selectStockSheet$1=SQL.from("SELECT ticker, incomeType, name, price, dividend, notes  FROM stock_view  ORDER BY ticker");
const selectTradeSheet$1=SQL.from("SELECT person, account, ticker, date, qty, cost, gain  FROM trade_view  ORDER BY person, account, ticker, tradeId");

let opened;

const DB_VERSION = 2;

function open () {
  if (opened) return
  opened = true;

  const dbFile = process.env.DB || join(homedir(), '.databases', 'findb.sqlite');
  const db = new SQLite(dbFile);
  SQL.attach(db);
  ddl.run();
  const version = db
    .prepare('select version from dbversion')
    .pluck()
    .get();
  if (version !== DB_VERSION) {
    throw new Error('Wrong version of db: ' + dbFile)
  }
}

const insertStocks = SQL.transaction(stocks => {
  for (const { ticker, name, incomeType, notes, source } of stocks) {
    insertStock.run({
      ticker,
      name,
      incomeType: incomeType || null,
      notes: notes || null,
      source
    });
  }
});

const insertPrices = SQL.transaction(prices => {
  for (const { ticker, name, price, source } of prices) {
    insertPrice.run({
      ticker,
      name,
      price: price.digits,
      priceFactor: price.factor,
      source
    });
  }
  clearOldPrices.run(7);
});

const insertDividends = SQL.transaction(divs => {
  for (const { ticker, dividend, source } of divs) {
    insertDividend.run({
      ticker,
      dividend: dividend ? dividend.digits : null,
      dividendFactor: dividend ? dividend.factor : null,
      source
    });
  }
  clearOldDividends.run(7);
});

const insertPositions = SQL.transaction(positions => {
  clearPositions.run();
  for (const { ticker, person, account, qty, source } of positions) {
    insertPosition.run({
      ticker,
      person,
      account,
      qty: qty ? qty.digits : 0,
      qtyFactor: qty ? qty.factor : 1,
      source
    });
  }
});

const insertTrades = SQL.transaction((account, trades) => {
  clearTrades.run(account);
  for (const trade of trades) {
    const {
      ticker,
      account,
      person,
      seq,
      date,
      qty,
      cost,
      gain,
      notes,
      source
    } = trade;
    insertTrade.run({
      ticker,
      account,
      person,
      seq,
      date,
      qty: qty ? qty.digits : null,
      qtyFactor: qty ? qty.factor : null,
      cost: cost ? cost.digits : null,
      costFactor: cost ? cost.factor : null,
      gain: gain ? gain.digits : null,
      gainFactor: gain ? gain.factor : null,
      notes: notes || null,
      source
    });
  }
  deleteTrades.run();
});

function selectActiveStocks () {
  return selectActiveStocks$1.pluck().all()
}

function selectPositionSheet () {
  return selectPositionSheet$1.all()
}

function selectTradeSheet () {
  return selectTradeSheet$1.all()
}

function selectStockSheet () {
  return selectStockSheet$1.all()
}

const allColours = (
  '20,21,26,27,32,33,38,39,40,41,42,43,44,45,56,57,62,63,68,69,74,75,76,' +
  '77,78,79,80,81,92,93,98,99,112,113,128,129,134,135,148,149,160,161,' +
  '162,163,164,165,166,167,168,169,170,171,172,173,178,179,184,185,196,' +
  '197,198,199,200,201,202,203,204,205,206,207,208,209,214,215,220,221'
)
  .split(',')
  .map(x => parseInt(x, 10));

const painters = [];

function makePainter (n) {
  const CSI = '\x1b[';
  const set = CSI + (n < 8 ? n + 30 + ';22' : '38;5;' + n + ';1') + 'm';
  const reset = CSI + '39;22m';
  return s => {
    if (!s.includes(CSI)) return set + s + reset
    return removeExcess(set + s.replaceAll(reset, reset + set) + reset)
  }
}

function painter (n) {
  if (painters[n]) return painters[n]
  painters[n] = makePainter(n);
  return painters[n]
}

// eslint-disable-next-line no-control-regex
const rgxDecolour = /(^|[^\x1b]*)((?:\x1b\[[0-9;]+m)|$)/g;
function truncate (string, max) {
  max -= 2; // leave two chars at end
  if (string.length <= max) return string
  const parts = [];
  let w = 0;
  for (const [, txt, clr] of string.matchAll(rgxDecolour)) {
    parts.push(txt.slice(0, max - w), clr);
    w = Math.min(w + txt.length, max);
  }
  return removeExcess(parts.join(''))
}

// eslint-disable-next-line no-control-regex
const rgxSerialColours = /(?:\x1b\[[0-9;]+m)+(\x1b\[[0-9;]+m)/g;
function removeExcess (string) {
  return string.replaceAll(rgxSerialColours, '$1')
}

function randomColour () {
  const n = Math.floor(Math.random() * allColours.length);
  return allColours[n]
}

const colours = {
  black: 0,
  red: 1,
  green: 2,
  yellow: 3,
  blue: 4,
  magenta: 5,
  cyan: 6,
  white: 7
};

const CLEAR_LINE = '\r\x1b[0K';

const state = {
  dirty: false,
  width: process.stdout && process.stdout.columns,
  /* c8 ignore next */
  level: process.env.LOGLEVEL ? parseInt(process.env.LOGLEVEL, 10) : undefined,
  write: process.stdout.write.bind(process.stdout)
};

process.stdout &&
  process.stdout.on('resize', () => (state.width = process.stdout.columns));

function _log (
  args,
  { newline = true, limitWidth, prefix = '', level, colour }
) {
  if (level && (!state.level || state.level < level)) return
  const msg = format(...args);
  let string = prefix + msg;
  if (colour != null) string = painter(colour)(string);
  if (limitWidth) string = truncate(string, state.width);
  if (newline) string = string + '\n';
  if (state.dirty) string = CLEAR_LINE + string;
  state.dirty = !newline && !!msg;
  state.write(string);
}

function makeLogger (base, changes = {}) {
  const baseOptions = base ? base._preset : {};
  const options = {
    ...baseOptions,
    ...changes,
    prefix: (baseOptions.prefix || '') + (changes.prefix || '')
  };
  const configurable = true;
  const fn = (...args) => _log(args, options);
  const addLevel = level => makeLogger(fn, { level });
  const addColour = c =>
    makeLogger(fn, { colour: c in colours ? colours[c] : randomColour() });
  const addPrefix = prefix => makeLogger(fn, { prefix });
  const status = () => makeLogger(fn, { newline: false, limitWidth: true });

  const colourFuncs = Object.fromEntries(
    Object.entries(colours).map(([name, n]) => [
      name,
      { value: painter(n), configurable }
    ])
  );

  return Object.defineProperties(fn, {
    _preset: { value: options, configurable },
    _state: { value: state, configurable },
    name: { value: 'log', configurable },
    level: { value: addLevel, configurable },
    colour: { value: addColour, configurable },
    prefix: { value: addPrefix, configurable },
    status: { get: status, configurable },
    ...colourFuncs
  })
}

const log = makeLogger();

function once (fn) {
  function f (...args) {
    if (f.called) return f.value
    f.value = fn(...args);
    f.called = true;
    return f.value
  }

  if (fn.name) {
    Object.defineProperty(f, 'name', { value: fn.name, configurable: true });
  }

  return f
}

const epochStartInSerial = 25569;
const msInDay = 24 * 60 * 60 * 1000;
const msInMinute = 60 * 1000;

class SerialDate {
  static fromSerial (n) {
    return new SerialDate(n)
  }

  static fromUTCms (ms) {
    return SerialDate.fromSerial(ms / msInDay + epochStartInSerial)
  }

  static fromUTCDate (d) {
    return SerialDate.fromUTCms(d.getTime())
  }

  static fromParts (parts) {
    parts = [...parts, 0, 0, 0, 0, 0, 0, 0].slice(0, 7);
    parts[1]--;
    return SerialDate.fromUTCms(Date.UTC(...parts))
  }

  static fromLocalDate (d) {
    return SerialDate.fromUTCms(
      d.getTime() - d.getTimezoneOffset() * msInMinute
    )
  }

  constructor (serial) {
    this.serial = serial;
    Object.freeze(this);
  }

  utcMs () {
    return Math.round((this.serial - epochStartInSerial) * msInDay)
  }

  utcDate () {
    return new Date(this.utcMs())
  }

  parts () {
    const d = this.utcDate();
    return [
      d.getUTCFullYear(),
      d.getUTCMonth() + 1,
      d.getUTCDate(),
      d.getUTCHours(),
      d.getUTCMinutes(),
      d.getUTCSeconds(),
      d.getUTCMilliseconds()
    ]
  }

  localDate () {
    const parts = this.parts();
    parts[1]--;
    return new Date(...parts)
  }
}

const SCOPES$1 = {
  rw: ['https://www.googleapis.com/auth/spreadsheets'],
  ro: ['https://www.googleapis.com/auth/spreadsheets.readonly']
};

const scopes = SCOPES$1;
const toDate = s => SerialDate.fromSerial(s).localDate();
const toSerial = d => SerialDate.fromLocalDate(d).serial;

async function getRange ({ sheet, range, ...options }) {
  const sheets = await getSheetAPI(options);

  const query = {
    spreadsheetId: sheet,
    range,
    valueRenderOption: 'UNFORMATTED_VALUE'
  };

  const response = await sheets.spreadsheets.values.get(query);

  if (response.status !== 200) {
    throw Object.assign(Error('Failed to read sheet'), { response })
  }
  return response.data.values
}

async function updateRange ({ sheet, range, data, ...options }) {
  const sheets = await getSheetAPI(options);

  data = data.map(row =>
    row.map(val => (val instanceof Date ? toSerial(val) : val))
  );

  const query = {
    spreadsheetId: sheet,
    range,
    valueInputOption: 'RAW',
    resource: {
      range,
      majorDimension: 'ROWS',
      values: data
    }
  };
  const response = await sheets.spreadsheets.values.update(query);

  if (response.status !== 200) {
    throw Object.assign(Error('Failed to update sheet'), { response })
  }
}

const getSheetAPI = once(async function getSheetAPI ({
  credentials = 'credentials.json',
  scopes = SCOPES$1.ro
} = {}) {
  const sheetsApi = await import('@googleapis/sheets');
  if (credentials) {
    process.env.GOOGLE_APPLICATION_CREDENTIALS = credentials;
  }

  const auth = new sheetsApi.auth.GoogleAuth({ scopes });
  const authClient = await auth.getClient();
  return sheetsApi.sheets({ version: 'v4', auth: authClient })
});

const SCOPES = {
  rw: ['https://www.googleapis.com/auth/drive'],
  ro: ['https://www.googleapis.com/auth/drive.readonly']
};

async function * list ({ folder, ...options }) {
  const drive = await getDriveAPI(options);
  const query = {
    fields: 'nextPageToken, files(id, name, mimeType, parents)'
  };

  if (folder) query.q = `'${folder}' in parents`;

  let pResponse = drive.files.list(query);

  while (pResponse) {
    const response = await pResponse;
    const { status, data } = response;
    if (status !== 200) {
      throw Object.assign(new Error('Bad result reading folder'), { response })
    }

    // fetch the next one if there is more
    if (data.nextPageToken) {
      query.pageToken = data.nextPageToken;
      pResponse = drive.files.list(query);
    } else {
      pResponse = null;
    }

    for (const file of data.files) {
      yield file;
    }
  }
}

const getDriveAPI = once(async function getDriveAPI ({
  credentials = 'credentials.json',
  scopes = SCOPES.ro
} = {}) {
  const driveApi = await import('@googleapis/drive');
  if (credentials) {
    process.env.GOOGLE_APPLICATION_CREDENTIALS = credentials;
  }

  const auth = new driveApi.auth.GoogleAuth({ scopes });
  const authClient = await auth.getClient();
  return driveApi.drive({ version: 'v3', auth: authClient })
});

const INVESTMENTS_FOLDER = '0B_zDokw1k2L7VjBGcExJeUxLSlE';

const locateSheets = once(async function locateSheets () {
  const m = new Map();
  const files = list({ folder: INVESTMENTS_FOLDER });
  for await (const file of files) {
    m.set(file.name, file);
  }
  return m
});

async function getSheetData (sheetName, range) {
  const sheetList = await locateSheets();
  const sheet = sheetList.get(sheetName).id;

  return getRange({ sheet, range, scopes: scopes.rw })
}

async function putSheetData (sheetName, range, data) {
  const sheetList = await locateSheets();
  const sheet = sheetList.get(sheetName).id;

  await updateRange({ sheet, range, data, scopes: scopes.rw });
}

async function overwriteSheetData (sheetName, range, data) {
  const currData = await getSheetData(sheetName, range);
  while (data.length < currData.length) {
    data.push(data[0].map(() => ''));
  }

  const newRange = range.replace(/\d+$/, '') + (data.length + 1);
  await putSheetData(sheetName, newRange, data);
}

const debug$7 = log
  .prefix('import:stocks:')
  .colour()
  .level(2);

const source$1 = {
  name: 'Stocks',
  range: 'Stocks!A:D'
};

async function importStocks (opts) {
  const rows = await getSheetData(source$1.name, source$1.range);

  const attrs = rows.shift();
  const validRow = row => !!row[0];
  const rowAttribs = row => attrs.map((k, ix) => [k, row[ix]]);
  const validAttribs = kvs => kvs.filter(([k]) => !!k);
  const makeObject = kvs => Object.fromEntries(kvs);

  const data = rows
    .filter(validRow)
    .map(rowAttribs)
    .map(validAttribs)
    .map(makeObject)
    .map(o => ({ ...o, source: 'sheet:stocks' }));

  insertStocks(data);

  debug$7('Loaded %d records from stocks', data.length);
}

var RGX = /([^{]*?)\w(?=\})/g;

var MAP = {
	YYYY: 'getFullYear',
	YY: 'getYear',
	MM: function (d) {
		return d.getMonth() + 1;
	},
	DD: 'getDate',
	HH: 'getHours',
	mm: 'getMinutes',
	ss: 'getSeconds',
	fff: 'getMilliseconds'
};

function tinydate (str, custom) {
	var parts=[], offset=0;

	str.replace(RGX, function (key, _, idx) {
		// save preceding string
		parts.push(str.substring(offset, idx - 1));
		offset = idx += key.length + 1;
		// save function
		parts.push(custom && custom[key] || function (d) {
			return ('00' + (typeof MAP[key] === 'string' ? d[MAP[key]]() : MAP[key](d))).slice(-key.length);
		});
	});

	if (offset !== str.length) {
		parts.push(str.substring(offset));
	}

	return function (arg) {
		var out='', i=0, d=arg||new Date();
		for (; i<parts.length; i++) {
			out += (typeof parts[i]==='string') ? parts[i] : parts[i](d);
		}
		return out;
	};
}

const customInspect = Symbol.for('nodejs.util.inspect.custom');
const div = (x, y) => {
  const pos = x >= 0n ? y > 0n : y < 0n;
  x = x < 0n ? -x : x;
  y = y < 0n ? -y : y;
  const r = x % y;
  const n = x / y + (r * 2n >= y ? 1n : 0n);
  return pos ? n : -n
};
/* c8 ignore next */
const rgxNumber = /^-?\d+(?:\.\d+)?$/;

const synonyms = {
  withPrec: 'withPrecision',
  withDP: 'withPrecision',
  toJSON: 'toString'
};

function decimal (x, opts = {}) {
  if (x instanceof Decimal) return x
  if (typeof x === 'bigint') return new Decimal(x, 0)
  if (typeof x === 'number') {
    if (Number.isInteger(x)) return new Decimal(BigInt(x), 0)
    x = x.toString();
  }
  if (typeof x !== 'string') throw new TypeError('Invalid number: ' + x)
  if (!rgxNumber.test(x)) throw new TypeError('Invalid number: ' + x)
  const i = x.indexOf('.');
  if (i > -1) {
    x = x.replace('.', '');
    return new Decimal(BigInt(x), x.length - i)
  } else {
    return new Decimal(BigInt(x), 0)
  }
}
decimal.from = function from ({ digits, precision, factor }) {
  if (precision == null) {
    precision = 0;
    while (getFactor(precision) < factor) precision++;
  }
  return new Decimal(BigInt(digits), precision)
};

decimal.isDecimal = function isDecimal (d) {
  return d instanceof Decimal
};

class Decimal {
  constructor (digs, prec) {
    this._d = digs;
    this._p = prec;
    Object.freeze(this);
  }

  [customInspect] (depth, opts) {
    /* c8 ignore next */
    if (depth < 0) return opts.stylize('[Decimal]', 'number')
    return `Decimal { ${opts.stylize(this.toString(), 'number')} }`
  }

  get digits () {
    return this._d
  }

  get precision () {
    return this._p
  }

  get factor () {
    return getFactor(this._p)
  }

  toNumber () {
    const factor = getFactor(this._p);
    return Number(this._d) / Number(factor)
  }

  toString () {
    const neg = this._d < 0n;
    const p = this._p;
    const d = neg ? -this._d : this._d;
    let t = d.toString().padStart(p + 1, '0');
    if (p) t = t.slice(0, -p) + '.' + t.slice(-p);
    return neg ? '-' + t : t
  }

  withPrecision (p) {
    const prec = this._p;
    if (prec === p) return this
    if (p > prec) {
      const f = getFactor(p - prec);
      return new Decimal(this._d * f, p)
    } else {
      const f = getFactor(prec - p);
      return new Decimal(div(this._d, f), p)
    }
  }

  neg () {
    return new Decimal(-this._d, this._p)
  }

  add (other) {
    other = decimal(other);
    if (other._p > this._p) return other.add(this)
    other = other.withPrecision(this._p);
    return new Decimal(this._d + other._d, this._p)
  }

  sub (other) {
    other = decimal(other);
    return this.add(other.neg())
  }

  mul (other) {
    other = decimal(other);
    // x*10^-a * y*10^-b = xy*10^-(a+b)
    return new Decimal(this._d * other._d, this._p + other._p).withPrecision(
      this._p
    )
  }

  div (other) {
    other = decimal(other);
    // x*10^-a / y*10^-b = (x/y)*10^-(a-b)
    return new Decimal(div(this._d * getFactor(other._p), other._d), this._p)
  }

  abs () {
    if (this._d >= 0n) return this
    return new Decimal(-this._d, this._p)
  }

  cmp (other) {
    other = decimal(other);
    if (this._p < other._p) return -other.cmp(this) || 0
    other = other.withPrecision(this._p);
    return this._d < other._d ? -1 : this._d > other._d ? 1 : 0
  }

  eq (other) {
    return this.cmp(other) === 0
  }

  normalise () {
    if (this._d === 0n) return this.withPrecision(0)
    for (let i = 0; i < this._p; i++) {
      if (this._d % getFactor(i + 1) !== 0n) {
        return this.withPrecision(this._p - i)
      }
    }
    return this.withPrecision(0)
  }
}

for (const k in synonyms) {
  Decimal.prototype[k] = Decimal.prototype[synonyms[k]];
}

const factors = [];
function getFactor (n) {
  n = Math.floor(n);
  return n in factors ? factors[n] : (factors[n] = 10n ** BigInt(n))
}

function importDecimal (x, prec) {
  if (x == null || x === '') return undefined
  const d = decimal(x);
  if (prec != null) return d.withPrecision(prec)
  return d
}

const plainDateString = tinydate('{YYYY}-{MM}-{DD}');

function importDate (x) {
  return typeof x === 'number' ? plainDateString(toDate(x)) : undefined
}

const debug$6 = log
  .prefix('import:portfolio:')
  .colour()
  .level(2);

const SOURCE = {
  name: 'Portfolio',
  range: 'Investments!A:AM'
};

const TICKER_COLUMN = 10; // column K
const ACCOUNT_COLUMN = 0; // column A
const ACCOUNT_LIST =
  'AJL,ISA;RSGG,ISA;AJL,Dealing;RSGG,Dealing;AJL,SIPP;RSGG,SIPP;RSGG,SIPP2';
const DIV_COLUMN = 26; // column AA
const accts = ACCOUNT_LIST.split(';')
  .map(code => code.split(','))
  .map(([person, account]) => ({ person, account }));

async function importPortfolio () {
  const rangeData = await getSheetData(SOURCE.name, SOURCE.range);

  await importDividends(rangeData);
  await importPositions(rangeData);
}

async function importDividends (rangeData) {
  const extractData = row => [row[TICKER_COLUMN], row[DIV_COLUMN]];
  const validTicker = ([ticker]) => !!ticker;
  const makeObj = ([ticker, dividend]) => ({
    ticker,
    dividend: importDecimal(dividend),
    source: 'sheets:portfolio'
  });

  const data = rangeData
    .map(extractData)
    .filter(validTicker)
    .map(makeObj);

  insertDividends(data);
  debug$6('Updated %d dividends', data.length);
}

async function importPositions (rangeData, opts) {
  const extractRow = row => [
    row[TICKER_COLUMN],
    accts,
    row.slice(ACCOUNT_COLUMN, ACCOUNT_COLUMN + accts.length)
  ];
  const validRow = ([ticker]) => !!ticker;
  const expandPositons = ([ticker, accts, qtys]) =>
    qtys.map((qty, i) => ({ ...accts[i], ticker, qty: importDecimal(qty, 0) }));
  const validPos = p => !!p.qty;

  const updates = rangeData
    .map(extractRow)
    .filter(validRow)
    .map(expandPositons)
    .flat(1)
    .filter(validPos)
    .map(o => ({ ...o, source: 'sheets:portfolio' }));

  insertPositions(updates);

  debug$6('%d positions updated', updates.length);
}

function sortBy (name, desc) {
  const fn = typeof name === 'function' ? name : x => x[name];
  const parent = typeof this === 'function' ? this : null;
  const direction = desc ? -1 : 1;
  sortFunc.thenBy = sortBy;
  return sortFunc

  function sortFunc (a, b) {
    return (parent && parent(a, b)) || direction * compare(a, b, fn)
  }

  function compare (a, b, fn) {
    const va = fn(a);
    const vb = fn(b);
    return va < vb ? -1 : va > vb ? 1 : 0
  }
}

const debug$5 = log
  .prefix('import:trades:')
  .colour()
  .level(2);

const source = {
  name: 'Trades',
  range: 'Trades!A2:F'
};

async function importTrades () {
  const rangeData = await getSheetData(source.name, source.range);

  const updates = [];
  const groups = getTradeGroups(rangeData);

  for (const group of groups) {
    if (!group.length) continue
    let seq = 0;
    for (const trade of group) {
      trade.seq = ++seq;
      trade.source = 'sheets:trades';
      updates.push(trade);
    }
  }

  insertTrades('Dealing', updates);
  debug$5('Updated %d positions with %d trades', groups.length, updates.length);
}

function getTradeGroups (rows) {
  const rawTrades = readTrades(rows);
  const sortedTrades = sortTrades(rawTrades);
  const groups = groupTrades(sortedTrades);
  addCosts(groups);

  return groups
}

function readTrades (rows) {
  const account = 'Dealing';
  let person;
  let ticker;
  const rowToObject = row => {
    const [person_, ticker_, date, qty, cost, notes] = row;
    return {
      person: (person = person_ || person),
      account,
      ticker: (ticker = ticker_ || ticker),
      date: importDate(date),
      qty: importDecimal(qty, 0),
      cost: importDecimal(cost, 2),
      notes
    }
  };

  const validTrade = t => t.person && t.ticker && t.date && (t.qty || t.cost);

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
  const key = t => `${t.person}_${t.account}_${t.ticker}`;
  const groups = [];
  let prev;
  let group;
  for (const trade of trades) {
    const k = key(trade);
    if (k !== prev) {
      prev = k;
      group = [];
      groups.push(group);
    }
    group.push(trade);
  }
  return groups
}

function addCosts (groups) {
  groups.forEach(group => group.forEach(buildPosition()));
}

function buildPosition () {
  const pos = { qty: decimal(0n), cost: decimal('0.00') };
  return trade => {
    const { qty, cost } = trade;
    if (qty && cost && qty.cmp(0n) > 0) {
      // buy
      pos.qty = pos.qty.add(qty);
      pos.cost = pos.cost.add(cost);
    } else if (qty && cost && qty.cmp(0n) < 0) {
      const prev = { ...pos };
      const proceeds = cost.abs();
      pos.qty = pos.qty.add(qty);
      const remain = prev.qty.eq(0n)
        ? decimal(0n)
        : pos.qty.withPrecision(9).div(prev.qty);
      pos.cost = prev.cost.mul(remain);
      trade.cost = prev.cost.sub(pos.cost).neg();
      trade.gain = proceeds.sub(trade.cost.abs());
    } else if (qty) {
      pos.qty = pos.qty.add(qty);
    } else if (cost) {
      pos.cost = pos.cost.add(cost);
    }
  }
}

function sleep (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

const rgxMain = (() => {
  const textElement = '(?<=^|>)' + '([^<]+)' + '(?=<)';
  const cdata = '<!\\[CDATA\\[';
  const comment = '<!--';
  const script = '<script(?= |>)';
  const specialElement = '(' + cdata + '|' + script + '|' + comment + ')';
  const tagElement = '(?:<)' + '([^>]*)' + '(?:>)';
  return new RegExp(textElement + '|' + specialElement + '|' + tagElement, 'g')
})();

const specials = {
  '<![CDATA[': { rgx: /]]>/, start: 9, end: 3, emit: 'cdata' },
  '<!--': { rgx: /-->/, start: 4, end: 3 },
  '<script': { rgx: /<\/script>/, start: 7, end: 9 }
};

const CHUNK = 1024;

const rgxTag = (() => {
  const maybeClose = '(\\/?)';
  const typeName = '(\\S+)';
  const elementType = `^${maybeClose}${typeName}`;
  const attrName = '(\\S+)';
  const attrValueDQ = '"([^"]*)"';
  const attrValueSQ = "'([^']*)'";
  const attrValueNQ = '(\\S+)';
  const attrValue = `(?:${attrValueDQ}|${attrValueSQ}|${attrValueNQ})`;
  const attr = `${attrName}\\s*=\\s*${attrValue}`;
  const selfClose = '(\\/)\\s*$';
  return new RegExp(`${elementType}|${attr}|${selfClose}`, 'g')
})();

class Parser {
  constructor (handler) {
    this.buffer = '';
    this.special = false;
    this.handler = handler;
  }

  write (text) {
    this.buffer += text;
    if (this.special) handleSpecial(this);
    else handle(this);
  }
}

function handle (p) {
  rgxMain.lastIndex = undefined;
  let consumed = 0;
  while (true) {
    const m = rgxMain.exec(p.buffer);
    if (!m) break
    const [, text, special, tag] = m;
    if (text) {
      consumed = m.index + text.length;
      p.handler({ text });
    } else if (tag) {
      consumed = m.index + tag.length + 2;
      p.handler(parseTag(tag));
    } else if (special) {
      p.special = special;
      const { start } = specials[special];
      consumed = m.index + start;
      p.buffer = p.buffer.slice(consumed);
      return handleSpecial(p)
    }
  }
  p.buffer = p.buffer.slice(consumed);
}

function handleSpecial (p) {
  const { rgx, end, emit } = specials[p.special];
  const match = rgx.exec(p.buffer);
  if (match) {
    const data = p.buffer.slice(0, match.index);
    p.buffer = p.buffer.slice(match.index + end);
    if (emit && data.length) p.handler({ [emit]: data });
    p.special = false;
    return handle(p)
  }
  if (p.buffer.length > CHUNK) {
    const data = p.buffer.slice(0, CHUNK);
    p.buffer = p.buffer.slice(CHUNK);
    if (emit) p.handler({ [emit]: data });
  }
}

function parseTag (tag) {
  if (tag.startsWith('!') || tag.startsWith('?')) return { meta: tag }
  const out = { type: '' };
  rgxTag.lastIndex = undefined;
  while (true) {
    const m = rgxTag.exec(tag);
    if (!m) return out
    const [, close, type, name, dq, sq, nq, selfClose] = m;
    if (type) {
      out.type = type;
      if (close) {
        out.close = true;
      } else {
        out.attrs = {};
      }
    } else if (name) {
      out.attrs[name] = dq || sq || nq;
    } else if (selfClose) {
      out.selfClose = true;
    }
  }
}

function ClosingParser (handler) {
  const parser = new Parser(ondata);
  const path = [];
  const write = parser.write.bind(parser);
  let depth = 0;

  return { write, path }

  function ondata (data) {
    data.depth = depth;
    const { type, close, selfClose, ...rest } = data;
    if (type && !close) {
      handler({ type, ...rest });
      if (selfClose) {
        handler({ type, close: true, depth });
      } else {
        path.push(type);
        depth++;
      }
    } else if (type && close) {
      while (path.length && path[path.length - 1] !== type) {
        const type = path.pop();
        depth--;
        handler({ type, close: true, depth });
      }
      if (depth) {
        path.pop();
        depth--;
      }
      handler({ type, close, depth });
    } else {
      handler(data);
    }
  }
}

class Scrapie {
  constructor (isChild) {
    if (!this.isChild) {
      const parser = new ClosingParser(this._ondata.bind(this));
      this.write = parser.write.bind(parser);
    }
    this._hooks = {};
  }

  on (event, callback) {
    if (event === 'text') {
      event = 'data';
      const cb = callback;
      callback = ({ text }) => text && cb(text);
    }
    const list = this._hooks[event];
    if (list) list.push(callback);
    else this._hooks[event] = [callback];
    return this
  }

  _emit (event, data) {
    const list = this._hooks[event];
    if (!list) return undefined
    for (let i = 0; i < list.length; i++) {
      list[i](data);
    }
  }

  _ondata (data) {
    this._emit('data', data);
  }

  when (fn) {
    if (typeof fn === 'string') fn = makeCondition(fn);
    return new SubScrapie(this, fn)
  }
}

class SubScrapie extends Scrapie {
  constructor (parent, condition) {
    super(true);
    parent.on('data', this._ondata.bind(this));
    this.write = parent.write;
    this._active = false;
    this._condition = condition;
  }

  _ondata (data) {
    if (this._active) {
      if (data.depth < this._activeDepth) {
        this._emit('exit', data);
        this._active = false;
      } else {
        this._emit('data', data);
      }
    } else {
      if (this._condition(data)) {
        this._emit('enter', data);
        this._active = true;
        this._activeDepth = data.depth + 1;
      }
    }
  }
}

function makeCondition (string) {
  if (string.includes('.')) {
    const [t, cls] = string.split('.');
    return ({ type, attrs }) =>
      type === t && attrs && attrs.class && attrs.class.includes(cls)
  }
  const t = string;
  return ({ type }) => type === t
}

const USER_AGENT =
  'Mozilla/5.0 (X11; CrOS x86_64 13729.56.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.95 Safari/537.36';

tinydate(
  '{YYYY}-{MM}-{DD}T{HH}:{mm}:{ss}.{fff}{TZ}',
  { TZ: getTZString }
);

function getTZString (d) {
  const o = d.getTimezoneOffset();
  const a = Math.abs(o);
  const s = o < 0 ? '+' : '-';
  const h = ('0' + Math.floor(a / 60)).slice(-2);
  const m = ('0' + (a % 60)).slice(-2);
  return s + h + ':' + m
}

function get (url) {
  return new Promise((resolve, reject) => {
    const req = get$1(url, { headers: { 'User-Agent': USER_AGENT } }, res => {
      const { statusCode } = res;
      if (statusCode >= 400) {
        const { statusMessage, headers } = res;
        return reject(
          Object.assign(new Error(res.statusMessage), {
            statusMessage,
            statusCode,
            headers,
            url
          })
        )
      }
      resolve(res);
    });
    req.on('error', reject);
  })
}

const debug$4 = log
  .prefix('fetch:lse:')
  .colour()
  .level(3);

function fetchIndex (indexName) {
  // ftse-all-share
  // ftse-aim-all-share
  const url = `https://www.lse.co.uk/share-prices/indices/${indexName}/constituents.html`;
  return fetchCollection(
    url,
    'sp-constituents__table',
    `lse:index:${indexName}`
  )
}

function fetchSector (sectorName) {
  // alternative-investment-instruments
  const url = `https://www.lse.co.uk/share-prices/sectors/${sectorName}/constituents.html`;
  return fetchCollection(url, 'sp-sectors__table', `lse:sector:${sectorName}`)
}

async function * fetchCollection (url, collClass, source) {
  await sleep(500);

  let count = 0;
  const items = [];
  const addItem = data => {
    const { name, ticker } = extractNameAndTicker(data[0]);
    const price = extractPriceInPence(data[1]);
    if (price) {
      items.push({ ticker, name, price, source });
      count++;
    }
  };

  let row;

  const scrapie = new Scrapie();
  scrapie
    .when('table.' + collClass)
    .when('tr')
    .on('enter', () => (row = []))
    .on('exit', () => row.length >= 2 && addItem(row))
    .when('td')
    .on('text', t => row.push(t));

  const data = await get(url);
  data.setEncoding('utf8');

  for await (const chunk of data) {
    scrapie.write(chunk);
    yield * items.splice(0);
  }

  debug$4('Read %d items from %s', count, source);
}

async function fetchPrice (ticker) {
  await sleep(500);

  const url = [
    'https://www.lse.co.uk/SharePrice.asp',
    `?shareprice=${ticker.padEnd(3, '.')}`
  ].join('');

  const item = {
    ticker,
    name: '',
    price: undefined,
    source: 'lse:share'
  };

  const scrapie = new Scrapie();

  const whenBid = ({ type, attrs }) =>
    type === 'span' && attrs && attrs['data-field'] === 'BID';

  scrapie.when('h1.title__title').on('text', t => {
    item.name = item.name || t.replace(/ Share Price.*/, '');
  });

  scrapie.when(whenBid).on('text', t => {
    item.price = item.price || extractPriceInPence(t);
  });

  const data = await get(url);
  data.setEncoding('utf8');

  for await (const chunk of data) {
    scrapie.write(chunk);
  }

  if (!item.price) {
    console.error('failed to fetch price from lse:share for %s', ticker);
  } else {
    debug$4('fetched %s from lse:share', ticker);
  }

  return item
}

function extractNameAndTicker (text) {
  const re = /(.*)\s+\(([A-Z0-9.]{2,4})\)$/;
  const m = re.exec(text);
  if (!m) return {}
  const [, name, ticker] = m;
  return { name, ticker: ticker.replace(/\.+$/, '') }
}

const hundred = decimal(100);
function extractPriceInPence (text) {
  return decimal(text.replace(/[,\s]/g, ''))
    .withPrecision(6)
    .div(hundred)
    .normalise()
}

const debug$3 = log
  .prefix('fetch:')
  .colour()
  .level(2);

// first try to load prices via collections - indices and sectors
const attempts = [
  ['ftse-all-share', fetchIndex],
  ['ftse-aim-all-share', fetchIndex],
  ['closed-end-investments', fetchSector]
];

async function fetchPrices () {
  const needed = new Set(selectActiveStocks());
  const updates = [];
  for await (const item of getPrices(needed)) {
    if (item.price) updates.push(item);
  }

  insertPrices(updates);
}

async function * getPrices (tickers) {
  const needed = new Set(tickers);
  const isNeeded = ({ ticker }) => needed.delete(ticker);

  for (const [name, fetchFunc] of attempts) {
    let n = 0;
    for await (const price of fetchFunc(name)) {
      if (!isNeeded(price)) continue
      n++;
      yield price;
    }
    debug$3('%d prices from %s', n, name);

    if (!needed.size) return
  }

  // now pick up the remaining ones
  for (const ticker of needed) {
    yield await fetchPrice(ticker);
  }
  debug$3('%d prices individually: %s', needed.size, [...needed].join(', '));
}

const debug$2 = log
  .prefix('export:positions:')
  .colour()
  .level(2);

const positions = { name: 'Positions', range: 'Positions!A2:I' };
const timestamp = { name: 'Positions', range: 'Positions!K1' };

async function exportPositions (opts) {
  const data = selectPositionSheet().map(makePositionRow);
  await overwriteSheetData(positions.name, positions.range, data);
  await putSheetData(timestamp.name, timestamp.range, [[new Date()]]);
  debug$2('position sheet updated');
}

function makePositionRow ({
  ticker,
  person,
  account,
  qty,
  price,
  dividend,
  yield: _yield,
  value,
  income
}) {
  return [
    ticker,
    person,
    account,
    qty || 0,
    price || 0,
    dividend || 0,
    _yield || 0,
    value || 0,
    income || 0
  ]
}

function makeCSV (arr) {
  return (
    arr
      .map(v => {
        if (typeof v === 'number') return v.toString()
        if (v == null) return ''
        return '"' + v.toString().replaceAll('"', '""') + '"'
      })
      .join(',') + '\n'
  )
}

function exportDate (x) {
  if (typeof x !== 'string') return x
  const m = /^(\d\d\d\d)-(\d\d)-(\d\d)/.exec(x);
  if (!m) return x
  const parts = m.slice(1).map(x => Number(x));
  return SerialDate.fromParts(parts).serial
}

const debug$1 = log
  .prefix('export:trades:')
  .colour()
  .level(2);

const trades = { name: 'Positions', range: 'Trades!A2:G' };

async function exportTrades (opts) {
  const data = selectTradeSheet().map(makeTradeRow);

  await overwriteSheetData(trades.name, trades.range, data);
  debug$1('trades sheet updated');
}

function makeTradeRow (t) {
  return [
    t.person,
    t.account,
    t.ticker,
    exportDate(t.date),
    t.qty || 0,
    t.cost || 0,
    t.gain || 0
  ]
}

function speedo ({
  total,
  interval = 250,
  windowSize = 40
} = {}) {
  let readings;
  let start;
  return Object.assign(transform, { current: 0, total, update, done: false })

  async function * transform (source) {
    start = Date.now();
    readings = [[start, 0]];
    const int = setInterval(update, interval);
    try {
      for await (const chunk of source) {
        transform.current += chunk.length;
        yield chunk;
      }
      transform.total = transform.current;
      update(true);
    } finally {
      clearInterval(int);
    }
  }

  function update (done = false) {
    if (transform.done) return
    const { current, total } = transform;
    const now = Date.now();
    const taken = now - start;
    readings = [...readings, [now, current]].slice(-windowSize);
    const first = readings[0];
    const wl = current - first[1];
    const wt = now - first[0];
    const rate = 1e3 * (done ? total / taken : wl / wt);
    const percent = Math.round((100 * current) / total);
    const eta = done || !total ? 0 : (1e3 * (total - current)) / rate;
    Object.assign(transform, { done, taken, rate, percent, eta });
  }
}

// import assert from 'assert/strict'
function throttle (options) {
  if (typeof options !== 'object') options = { rate: options };
  const { chunkTime = 100, windowSize = 30 } = options;
  const rate = getRate(options.rate);
  return async function * throttle (source) {
    let window = [[0, Date.now()]];
    let bytes = 0;
    let chunkBytes = 0;
    const chunkSize = Math.max(1, Math.ceil((rate * chunkTime) / 1e3));
    for await (let data of source) {
      while (data.length) {
        const chunk = data.slice(0, chunkSize - chunkBytes);
        data = data.slice(chunk.length);
        chunkBytes += chunk.length;
        if (chunkBytes < chunkSize) {
          // assert.equal(data.length, 0)
          yield chunk;
          continue
        }
        bytes += chunkSize;
        // assert.equal(chunkBytes, chunkSize)
        chunkBytes = 0;
        const now = Date.now();
        const first = window[0];
        const eta = first[1] + (1e3 * (bytes - first[0])) / rate;
        window = [...window, [bytes, Math.max(now, eta)]].slice(-windowSize);
        if (now < eta) {
          await delay(eta - now);
        }
        yield chunk;
      }
    }
  }
}

function getRate (val) {
  const n = (val + '').toLowerCase();
  if (!/^\d+[mk]?$/.test(n)) throw new Error(`Invalid rate: ${val}`)
  const m = n.endsWith('m') ? 1024 * 1024 : n.endsWith('k') ? 1024 : 1;
  return parseInt(n) * m
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

function progressStream ({
  onProgress,
  interval = 1000,
  ...rest
} = {}) {
  return async function * transform (source) {
    const int = setInterval(report, interval);
    let bytes = 0;
    let done = false;
    try {
      for await (const chunk of source) {
        bytes += chunk.length;
        yield chunk;
      }
      done = true;
      report();
    } finally {
      clearInterval(int);
    }

    function report () {
      onProgress && onProgress({ bytes, done, ...rest });
    }
  }
}

async function hashFile (filename, { algo = 'md5', enc = 'hex' } = {}) {
  const hasher = createHash(algo);
  for await (const chunk of createReadStream(filename)) {
    hasher.update(chunk);
  }
  return hasher.digest(enc)
}

function parse (uri) {
  const u = new URL(uri);
  if (u.protocol !== 'gs:') throw new Error('Invalid protocol')
  const bucket = u.hostname;
  const file = u.pathname.replace(/^\//, '');
  return { bucket, file }
}

async function upload (src, dest, options = {}) {
  const { onProgress, progressInterval = 1000, rateLimit, acl } = options;
  const { bucket: bucketName, file: fileName } = parse(dest);
  const { contentType, ...metadata } = await getLocalMetadata(src);
  const storage = await getStorageAPI();
  const bucket = storage.bucket(bucketName);
  const file = bucket.file(fileName);

  const speedo$1 = speedo({ total: metadata.size });
  const writeOptions = {
    public: acl === 'public',
    private: acl === 'private',
    resumable: metadata.size > 5e6,
    metadata: {
      contentType: metadata.contentType,
      metadata: packMetadata(metadata)
    }
  };

  await pipeline(
    ...[
      createReadStream(src),
      rateLimit && throttle(rateLimit),
      onProgress && speedo$1,
      onProgress &&
        progressStream({ onProgress, interval: progressInterval, speedo: speedo$1 }),
      file.createWriteStream(writeOptions)
    ].filter(Boolean)
  );
}

async function getLocalMetadata (file) {
  const { mtimeMs, ctimeMs, atimeMs, size, mode } = await stat(file);
  const md5 = await hashFile(file);
  const contentType = mime.getType(extname(file));
  const defaults = { uid: 1000, gid: 1000, uname: 'alan', gname: 'alan' };
  return {
    ...defaults,
    mtime: Math.floor(mtimeMs),
    ctime: Math.floor(ctimeMs),
    atime: Math.floor(atimeMs),
    size,
    mode,
    md5,
    contentType
  }
}

function packMetadata (obj, key = 'gsjs') {
  return {
    [key]: Object.keys(obj)
      .sort()
      .map(k => [k, obj[k]])
      .filter(([, v]) => v != null)
      .map(([k, v]) => `${k}:${v}`)
      .join('/')
  }
}

const getStorageAPI = once(async function getStorageAPI ({
  credentials = 'credentials.json'
} = {}) {
  const { Storage } = await import('@google-cloud/storage');
  if (credentials) {
    process.env.GOOGLE_APPLICATION_CREDENTIALS = credentials;
  }

  const storage = new Storage();
  return storage
});

const debug = log
  .prefix('export:stocks:')
  .colour()
  .level(2);

const STOCKS_URI = 'gs://finance-readersludlow/stocks.csv';
const TEMPFILE = '/tmp/stocks.csv';

async function exportStocks () {
  const data = selectStockSheet()
    .map(stockToRow)
    .map(makeCSV)
    .join('');

  await writeFile(TEMPFILE, data);
  await upload(TEMPFILE, STOCKS_URI, { acl: 'public' });
  await unlink(TEMPFILE);
  debug('stocks written to %s', STOCKS_URI);
}

function stockToRow (row) {
  const { ticker, incomeType, name, price, dividend, notes } = row;
  return [
    ticker,
    incomeType || '',
    name || '',
    price || 0,
    dividend || 0,
    notes || ''
  ]
}

async function main (opts) {
  open();
  const cmds = opts._;
  while (cmds.length) {
    const cmd = cmds.shift();
    if (cmd === 'import-stocks') {
      await importStocks().catch(bail);
    } else if (cmd === 'import-portfolio') {
      await importPortfolio().catch(bail);
    } else if (cmd === 'import-trades') {
      await importTrades().catch(bail);
    } else if (cmd === 'fetch-prices') {
      await fetchPrices().catch(bail);
    } else if (cmd === 'export-positions') {
      await exportPositions().catch(bail);
    } else if (cmd === 'export-trades') {
      await exportTrades().catch(bail);
    } else if (cmd === 'export-stocks') {
      await exportStocks().catch(bail);
    } else if (cmd === 'import') {
      cmds.unshift('import-stocks', 'import-portfolio', 'import-trades');
    } else if (cmd === 'fetch') {
      cmds.unshift('fetch-prices');
    } else if (cmd === 'export') {
      cmds.unshift('export-positions', 'export-trades', 'export-stocks');
    } else {
      console.error('No such command: %s', cmd);
      process.exit(1);
    }
  }
}

function bail (err) {
  console.error(err);
  process.exit(2);
}

const version = '1.2.1';
const opts = mri(process.argv.slice(2), {
  alias: {
    help: 'h',
    version: 'v'
  }
});
if (opts.version) {
  console.log('finupdate %s', version);
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
  );
} else {
  main(opts);
}
