-- insertDividend

-- inserts, updates or deletes a dividend record

-- params
--  ticker
--  dividend / factor
--  source

INSERT OR IGNORE INTO stock
    (ticker)
VALUES
    ($ticker);

-- Insert / update divi where we have one

INSERT INTO stock_dividend (
        stockId,
        dividend,
        dividendFactor,
        source
    )
    SELECT stockId,
        $dividend,
        $dividendFactor,
        $source
    FROM stock
    WHERE ticker = $ticker
      AND $dividend IS NOT NULL
ON CONFLICT DO UPDATE
    SET dividend        = excluded.dividend,
        dividendFactor  = excluded.dividendFactor,
        source          = excluded.source;

-- Delete divi where we are given null

DELETE FROM stock_dividend
    WHERE stockId IN (
        SELECT stockId
            FROM stock
            WHERE ticker = $ticker)
    AND $dividend IS NULL;
