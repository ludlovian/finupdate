-- clearPositions

-- zero out all non-zero positions

UPDATE position
  SET   qty       = 0,
        qtyFactor = 1,
        source    = NULL,
        updated   = datetime('now')
  WHERE qty != 0;
