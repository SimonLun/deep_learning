        select cast(TRADE_DT as signed) as Date, S_INFO_WINDCODE as IndexTicker, left(S_CON_WINDCODE, 6) as Ticker, I_WEIGHT as weight
        from wind.AINDEXHS300FREEWEIGHT
        WHERE S_INFO_WINDCODE in ('{0}')
        and TRADE_DT>='{1}' and TRADE_DT<='{2}'