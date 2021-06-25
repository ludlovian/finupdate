-- insertPrice
--
-- Params:
--  ticker
--  name
--  price / priceFactor
--  source

-- Insert stock if it doesn't exist
--
INSERT OR IGNORE INTO stock(ticker)
    VALUES ($ticker);

-- Update stock name if we don't already have it
--
UPDATE stock
    SET name = $name
  WHERE ticker = $ticker
    AND name   IS NULL;

-- Insert or update the price

INSERT INTO stock_price (
        stockId,
        price,
        priceFactor,
        source
    )
    SELECT stockId,
        $price,
        $priceFactor,
        $source
    FROM stock
    WHERE ticker = $ticker
ON CONFLICT DO UPDATE
    SET price       = excluded.price,
        priceFactor = excluded.priceFactor,
        source      = excluded.source;

