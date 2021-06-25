-- clearTrades

-- zeros out trades for the given account

-- params
--      account

UPDATE trade
  SET   qty     = NULL,
        cost    = NULL,
        gain    = NULL
  WHERE positionId IN (
    SELECT positionId
      FROM position
      WHERE accountId IN (
        SELECT accountId
          FROM account
          WHERE name = ?
      )
  );
