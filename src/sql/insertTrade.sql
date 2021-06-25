-- insert Trade

-- inserts or updates trades

-- params:
--      ticker
--      account
--      person
--      seq
--      date
--      qty / factor
--      cost / factor
--      gain / factor
--      notes
--      source

-- insert ticker, account, person if needed

INSERT OR IGNORE INTO stock (ticker)
    VALUES ($ticker);

INSERT OR IGNORE INTO person (name)
    VALUES ($person);

INSERT OR IGNORE INTO account (name)
    VALUES ($account);

-- insert zero position record if needed

INSERT OR IGNORE INTO position (
    personId,
    accountId,
    stockId,
    qty,
    qtyFactor
)
    SELECT
        w.personId,
        a.accountId,
        s.stockId,
        0,
        1
    FROM    person w,
            account a,
            stock s
    WHERE   w.name   = $person
    AND     a.name   = $account
    AND     s.ticker = $ticker;

-- Insert trade into trade

INSERT INTO trade (
    positionId,
    seq,
    date,
    qty,
    qtyFactor,
    cost,
    costFactor,
    gain,
    gainFactor,
    notes,
    source
)
    SELECT
        p.positionId,
        $seq,
        $date,
        $qty,
        $qtyFactor,
        $cost,
        $costFactor,
        $gain,
        $gainFactor,
        $notes,
        $source
    FROM       position p
    INNER JOIN person w USING (personId)
    INNER JOIN account a USING (accountId)
    INNER JOIN stock s USING (stockId)
    WHERE s.ticker  = $ticker
    AND   a.name    = $account
    AND   w.name    = $person

ON CONFLICT DO UPDATE
SET
    date        = excluded.date,
    qty         = excluded.qty,
    qtyFactor   = excluded.qtyFactor,
    cost        = excluded.cost,
    costFactor  = excluded.costFactor,
    gain        = excluded.gain,
    gainFactor  = excluded.gainFactor,
    notes       = excluded.notes,
    source      = excluded.source,
    updated     = excluded.updated;
