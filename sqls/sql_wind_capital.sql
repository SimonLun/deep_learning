select CAST({0} as signed) as Date, left(A.S_INFO_WINDCODE,6) as Ticker,
    B.TOT_SHR, B.S_SHARE_TOTALA, B.FLOAT_SHR, B.FLOAT_A_SHR from (
	    SELECT S_INFO_WINDCODE, max(CHANGE_DT) AS Date_ FROM wind.ASHARECAPITALIZATION
		where CHANGE_DT<='{0}' and left(S_INFO_WINDCODE,6) in (
			select C.S_INFO_CODE from wind.ASHAREDESCRIPTION C
			where not ((C.S_INFO_LISTDATE >'{1}') or (ifnull(C.S_INFO_DELISTDATE,'21000101')<'{0}'))

			UNION

            select K.S_INFO_CODE from wind.ASHAREDESCRIPTION K where K.S_INFO_COMPCODE in (
            select H.S_INFO_COMPCODE from (
                select S_INFO_COMPCODE, count(*) as cn from wind.ASHAREDESCRIPTION where S_INFO_LISTDATE is not null group by  S_INFO_COMPCODE) H
            WHERE H.cn>1)
			)
		group by S_INFO_WINDCODE
    ) A
	left join wind.ASHARECAPITALIZATION B
    on A.S_INFO_WINDCODE=B.S_INFO_WINDCODE and A.Date_=B.CHANGE_DT

	UNION

    select CAST(CHANGE_DT as signed) as Date, left(S_INFO_WINDCODE,6) as Ticker, TOT_SHR, S_SHARE_TOTALA,
    FLOAT_SHR, FLOAT_A_SHR from wind.ASHARECAPITALIZATION
    where CHANGE_DT>='{0}' and CHANGE_DT<='{1}'