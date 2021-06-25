PRAGMA journal_mode = WAL;
PRAGMA user_version = 2;
PRAGMA foreign_keys = TRUE;

CREATE TABLE IF NOT EXISTS stock(
    stockId INTEGER PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    name TEXT,
    incomeType TEXT,
    notes TEXT,
    source TEXT,
    updated TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stock_dividend(
    stockId INTEGER PRIMARY KEY REFERENCES stock(stockId),
    dividend INTEGER NOT NULL,
    dividendFactor INTEGER NOT NULL DEFAULT 1,
    source TEXT,
    updated TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS stock_price(
    stockId INTEGER PRIMARY KEY REFERENCES stock(stockId),
    price INTEGER NOT NULL,
    priceFactor INTEGER NOT NULL,
    source TEXT,
    updated TEXT NOT NULL DEFAULT (datetime('now'))
);

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
