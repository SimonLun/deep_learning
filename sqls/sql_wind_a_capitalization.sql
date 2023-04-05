select left(S_INFO_WINDCODE,6) as Ticker, cast(CHANGE_DT as signed) as Date, CAST(CHANGE_DT1 AS signed) as Date1
        from wind.ASHARECAPITALIZATION
        where (CHANGE_DT-CHANGE_DT1)!=0 and S_SHARE_CHANGEREASON!='SS'
        AND not("{0}">=CHANGE_DT or "{1}"<CHANGE_DT1)