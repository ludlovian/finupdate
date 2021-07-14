import { homedir } from 'os'
import { join } from 'path'

import SQLite from 'better-sqlite3'

import once from 'pixutil/once'

import { tidy, statement, transaction } from './util.mjs'

const DB_VERSION = 2

const db = once(() => {
  const dbFile = process.env.DB || join(homedir(), '.databases', 'findb.sqlite')
  const db = new SQLite(dbFile)
  db.exec(ddl)
  const version = db
    .prepare('select version from dbversion')
    .pluck()
    .get()
  if (version !== DB_VERSION) {
    throw new Error('Wrong version of db: ' + dbFile)
  }
  return db
})

export function sql (text) {
  return statement(tidy(text), { db })
}
sql.transaction = fn => transaction(fn, db)

const ddl = tidy(`
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = TRUE;

BEGIN TRANSACTION;

CREATE VIEW IF NOT EXISTS dbversion AS
    SELECT 2 AS version;

---- Stock static data -----------------

CREATE TABLE IF NOT EXISTS stock(
    stockId INTEGER PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    name TEXT,
    incomeType TEXT,
    notes TEXT,
    source TEXT,
    updated TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE VIEW IF NOT EXISTS stock_v AS
  SELECT ticker, name, incomeType, notes, source
  FROM stock;

CREATE TRIGGER IF NOT EXISTS stock_vi
  INSTEAD OF INSERT ON stock_v
BEGIN
    INSERT OR IGNORE INTO stock (ticker)
    VALUES (NEW.ticker);

    UPDATE stock
    SET   (name, incomeType, notes, source) =
            (NEW.name, NEW.incomeType, NEW.notes, NEW.source),
          updated = datetime('now')
    WHERE ticker = NEW.ticker;
END;

---- Stock dividends -------------------

CREATE TABLE IF NOT EXISTS stock_dividend(
    stockId INTEGER PRIMARY KEY REFERENCES stock(stockId),
    dividend INTEGER NOT NULL,
    dividendFactor INTEGER NOT NULL DEFAULT 1,
    source TEXT,
    updated TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE VIEW IF NOT EXISTS stock_dividend_v AS
  SELECT  s.ticker AS ticker,
          d.dividend AS dividend,
          d.dividendFactor AS dividendFactor,
          d.source AS source
  FROM    stock_dividend d
  JOIN    stock s USING (stockId);

CREATE TRIGGER IF NOT EXISTS stock_dividend_vi
  INSTEAD OF INSERT ON stock_dividend_v
BEGIN
  INSERT OR IGNORE INTO stock (ticker)
    VALUES (NEW.ticker);
  INSERT INTO stock_dividend
    (stockId, dividend, dividendFactor, source)
    SELECT stockId,
           NEW.dividend,
           NEW.dividendFactor,
           NEW.source
    FROM   stock
    WHERE  ticker = NEW.ticker
    AND    NEW.dividend IS NOT NULL
  ON CONFLICT DO UPDATE
    SET dividend       = excluded.dividend,
        dividendFactor = excluded.dividendFactor,
        source         = excluded.source,
        updated        = excluded.updated;
  DELETE FROM stock_dividend
    WHERE stockId IN
          ( SELECT stockId FROM stock
            WHERE ticker = NEW.ticker)
    AND   NEW.dividend IS NULL;
END;

---- Stock prices ----------------------

CREATE TABLE IF NOT EXISTS stock_price(
    stockId INTEGER PRIMARY KEY REFERENCES stock(stockId),
    price INTEGER NOT NULL,
    priceFactor INTEGER NOT NULL,
    source TEXT,
    updated TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE VIEW IF NOT EXISTS stock_price_v AS
  SELECT s.ticker      AS ticker,
         s.name        AS name,
         r.price       AS price,
         r.priceFactor AS priceFactor,
         r.source      AS source
  FROM   stock_price r
  JOIN   stock s USING (stockId);

CREATE TRIGGER IF NOT EXISTS stock_price_vi
  INSTEAD OF INSERT ON stock_price_v
BEGIN
  INSERT OR IGNORE INTO stock (ticker)
    VALUES (NEW.ticker);
  UPDATE stock
    SET   name = NEW.name
    WHERE ticker = NEW.ticker
    AND   name IS NULL;
  INSERT INTO stock_price
    (stockId, price, priceFactor, source)
    SELECT stockId,
           NEW.price,
           NEW.priceFactor,
           NEW.source
    FROM   stock
    WHERE  ticker = NEW.ticker
  ON CONFLICT DO UPDATE
    SET price       = excluded.price,
        priceFactor = excluded.priceFactor,
        source      = excluded.source,
        updated     = excluded.updated;
END;

---- Account and person ----------------

CREATE TABLE IF NOT EXISTS account(
    accountId INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
INSERT OR IGNORE INTO account
    (name)
VALUES
    ('Dealing'),
    ('ISA'),
    ('SIPP'),
    ('SIPP2');

CREATE TABLE IF NOT EXISTS person(
    personId INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);
INSERT OR IGNORE INTO person
    (name)
VALUES
    ('AJL'),
    ('RSGG');

---- Positions -------------------------

CREATE TABLE IF NOT EXISTS position(
    positionId INTEGER PRIMARY KEY,
    personId INTEGER NOT NULL REFERENCES person(personId),
    accountId INTEGER NOT NULL REFERENCES account(accountId),
    stockId INTEGER NOT NULL REFERENCES stock(stockId),
    qty INTEGER NOT NULL,
    qtyFactor INTEGER NOT NULL DEFAULT 1,
    source TEXT,
    updated TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (personId, accountId, stockId)
);

CREATE INDEX IF NOT EXISTS position_i1 ON position
  (personId);
CREATE INDEX IF NOT EXISTS position_i2 ON position
  (accountId);
CREATE INDEX IF NOT EXISTS position_i3 ON position
  (stockId);

CREATE VIEW IF NOT EXISTS position_v AS
  SELECT w.name AS person,
         a.name AS account,
         s.ticker AS ticker,
         p.qty AS qty,
         p.qtyFactor AS qtyFactor,
         p.source AS source
  FROM   position p
  JOIN   person w USING (personId)
  JOIN   account a USING (accountId)
  JOIN   stock s USING (stockId);

CREATE TRIGGER IF NOT EXISTS position_vi
  INSTEAD OF INSERT ON position_v
BEGIN
  INSERT OR IGNORE INTO stock (ticker)
    VALUES (NEW.ticker);
  INSERT OR IGNORE INTO account (name)
    VALUES (NEW.account);
  INSERT OR IGNORE INTO person (name)
    VALUES (NEW.person);
  INSERT INTO position
    (personId, accountId, stockId, qty, qtyFactor, source)
    SELECT w.personId,
           a.accountId,
           s.stockId,
           NEW.qty,
           NEW.qtyFactor,
           NEW.source
    FROM   person w, account a, stock s
    WHERE  w.name = NEW.person
    AND    a.name = NEW.account
    AND    s.ticker = NEW.ticker
  ON CONFLICT DO UPDATE
    SET qty       = excluded.qty,
        qtyFactor = excluded.qtyFactor,
        source    = excluded.source,
        updated   = excluded.updated;
END;

---- Trades ----------------------------

CREATE TABLE IF NOT EXISTS trade(
    tradeId INTEGER PRIMARY KEY,
    positionId INTEGER NOT NULL REFERENCES position(positionId),
    seq INTEGER NOT NULL,
    date TEXT NOT NULL,
    qty INTEGER,
    qtyFactor INTEGER,
    cost INTEGER,
    costFactor INTEGER,
    gain INTEGER,
    gainFactor INTEGER,
    notes TEXT,
    source TEXT,
    updated TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (positionId, seq)
);

CREATE VIEW IF NOT EXISTS trade_v AS
  SELECT  w.name AS person,
          a.name AS account,
          s.ticker AS ticker,
          t.seq AS seq,
          t.date AS date,
          t.qty AS qty,
          t.qtyFactor as qtyFactor,
          t.cost AS cost,
          t.costFactor AS costFactor,
          t.gain AS gain,
          t.gainFactor AS gainFactor,
          t.notes AS notes,
          t.source AS source
  FROM    trade t
  JOIN    position p USING (positionId)
  JOIN    account a ON a.accountId = p.accountId
  JOIN    person w ON w.personId = p.personId
  JOIN    stock s ON s.stockId = p.stockId;

CREATE TRIGGER IF NOT EXISTS trade_vi
  INSTEAD OF INSERT ON trade_v
BEGIN
  INSERT OR IGNORE INTO stock (ticker)
    VALUES (NEW.ticker);
  INSERT OR IGNORE INTO person (name)
    VALUES (NEW.person);
  INSERT OR IGNORE INTO account (name)
    VALUES (NEW.account);
  INSERT OR IGNORE INTO position
    (personId, accountId, stockId, qty, qtyFactor)
    SELECT w.personId,
           a.accountId,
           s.stockId,
           0,
           1
    FROM   person w, account a, stock s
    WHERE  w.name   = NEW.person
    AND    a.name   = NEW.account
    AND    s.ticker = NEW.ticker;
  INSERT INTO TRADE
    (positionId, seq, date, qty, qtyFactor, cost, costFactor,
      gain, gainFactor, notes, source)
    SELECT  p.positionId, NEW.seq, NEW.date, NEW.qty, NEW.qtyFactor,
              NEW.cost, NEW.costFactor, NEW.gain, NEW.gainFactor,
              NEW.notes, NEW.source
    FROM    position p
    JOIN    person w USING (personId)
    JOIN    account a USING (accountId)
    JOIN    stock s USING (stockId)
    WHERE   s.ticker = NEW.ticker
    AND     a.name   = NEW.account
    AND     w.name   = NEW.person
  ON CONFLICT DO UPDATE
    SET (date, qty, qtyFactor, cost, costFactor,
          gain, gainFactor, notes, source)
        = (NEW.date, NEW.qty, NEW.qtyFactor, NEW.cost, NEW.costFactor,
            NEW.gain, NEW.gainFactor, NEW.notes, NEW.source),
        updated = excluded.updated;
END;

---- Extract views ---------------------

CREATE VIEW IF NOT EXISTS stock_view AS
  SELECT
        s.stockId       AS stockId,
        s.ticker        AS ticker,
        s.name          AS name,
        s.incomeType    AS incomeType,
        s.notes         AS notes,
        CAST (d.dividend AS REAL) / 
            CAST (d.dividendFactor AS REAL)
                        AS dividend,
        CAST (p.price AS REAL) /
            CAST (p.priceFactor AS REAL)
                        AS price,
        CAST (d.dividend * p.priceFactor AS REAL) /
            CAST (p.price * d.dividendFactor AS REAL)
                        AS yield
  FROM stock s
  LEFT JOIN stock_dividend d USING (stockId)
  LEFT JOIN stock_price p USING (stockId)
  ORDER BY ticker;

CREATE VIEW IF NOT EXISTS position_view AS
  SELECT
        p.positionId    AS positionId,
        s.ticker        AS ticker,
        a.name          AS account,
        w.name          AS person,
        CAST (p.qty AS REAL) / CAST (p.qtyFactor AS REAL)
                        AS qty,
        round(p.qty * s.price, 2)
                        AS value,
        round(p.qty * s.dividend, 2)
                        AS income
  FROM position p
  INNER JOIN stock_view s USING (stockId)
  INNER JOIN account a USING (accountId)
  INNER JOIN person w USING (personId)
  WHERE qty != 0
  ORDER BY ticker, account, person;

CREATE VIEW IF NOT EXISTS trade_view AS
  SELECT
    t.tradeId           AS tradeId,
    s.ticker            AS ticker,
    a.name              AS account,
    w.name              AS person,
    t.date              AS date,
    CAST (t.qty AS REAL) / CAST (t.qtyFactor AS REAL)
                        AS qty,
    round(CAST (t.cost AS REAL) / CAST (t.costFactor AS REAL), 2)
                        AS cost,
    round(CAST (t.gain AS REAL) / CAST (t.gainFactor AS REAL), 2)
                        AS gain,
    t.notes             AS notes
  FROM  trade t,
        position p,
        account a,
        person w,
        stock s
  WHERE t.positionId    = p.positionId
  AND   p.accountId     = a.accountId
  AND   p.personId      = w.personId
  AND   p.stockId       = s.stockId
  ORDER BY ticker, account, person, t.seq;

COMMIT;
`)
