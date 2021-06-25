INSERT INTO stock
    (ticker, name, incomeType, notes, source)
VALUES
    ($ticker, $name, $incomeType, $notes, $source)
ON CONFLICT DO UPDATE
    SET name        = excluded.name,
        incomeType  = excluded.incomeType,
        notes       = excluded.notes,
        source      = excluded.source,
        updated     = excluded.updated;

