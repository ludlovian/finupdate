-- insertPosition

-- inserts or updates positions

-- params:
--      ticker
--      account
--      person
--      qty / factor
--      source

-- insert ticker, account & person if needed

INSERT OR IGNORE INTO stock (ticker)
    VALUES ($ticker);

INSERT OR IGNORE INTO person (name)
    VALUES ($person);

INSERT OR IGNORE INTO account (name)
    VALUES ($account);

-- Insert or update position record

INSERT INTO position (
    personId,
    accountId,
    stockId,
    qty,
    qtyFactor,
    source
)
    SELECT
        p.personId,
        a.accountId,
        s.stockId,
        $qty,
        $qtyFactor,
        $source
    FROM    person p,
            account a,
            stock s
    WHERE   p.name   = $person
    AND     a.name   = $account
    AND     s.ticker = $ticker

ON CONFLICT DO UPDATE
    SET qty         = excluded.qty,
        qtyFactor   = excluded.qtyFactor,
        source      = excluded.source,
        updated     = excluded.updated;
