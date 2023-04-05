 select {0} as Date, left(S_CON_WINDCODE, 6) as Ticker
        from wind.AINDEXMEMBERS where S_INFO_WINDCODE='{1}' and '{0}' between S_CON_INDATE and ifnull(S_CON_OUTDATE, '21000101')