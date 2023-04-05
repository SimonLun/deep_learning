select CAST({0} as signed) as Date, left(A.S_INFO_WINDCODE,6) as Ticker,
        B.S_SHARE_FREESHARES from (
            SELECT S_INFO_WINDCODE, max(CHANGE_DT1) AS Date_ FROM wind.ASHAREFREEFLOAT
            where CHANGE_DT1<='{0}' and left(S_INFO_WINDCODE,6) in (
                select C.S_INFO_CODE from wind.ASHAREDESCRIPTION C
                where not ((C.S_INFO_LISTDATE >'{1}') or (ifnull(C.S_INFO_DELISTDATE,'21000101')<'{0}')) )
            group by S_INFO_WINDCODE
        ) A
        left join wind.ASHAREFREEFLOAT B
        on A.S_INFO_WINDCODE=B.S_INFO_WINDCODE and A.Date_=B.CHANGE_DT1

        UNION

        select CAST(CHANGE_DT1 as signed) as Date, left(S_INFO_WINDCODE,6) as Ticker, S_SHARE_FREESHARES from wind.ASHAREFREEFLOAT
        where CHANGE_DT1>='{0}' and CHANGE_DT1<='{1}'