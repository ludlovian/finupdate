-- removes zero trades

DELETE FROM trade
  WHERE qty IS NULL
  AND   cost IS NULL
  AND   gain IS NULL;
