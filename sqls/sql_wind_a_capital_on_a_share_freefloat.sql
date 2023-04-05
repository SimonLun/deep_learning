select left(S_INFO_WINDCODE,6) as Ticker, cast(CHANGE_DT1 as signed) as Date, CAST(CHANGE_DT AS signed) as Date1
        from wind.ASHAREFREEFLOAT
        where (CHANGE_DT-CHANGE_DT1)!=0
        and not("{0}">=CHANGE_DT1 or "{1}"<CHANGE_DT)