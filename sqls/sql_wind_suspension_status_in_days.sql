
                select cast(S_DQ_SUSPENDDATE as signed) as Date, left(S_INFO_WINDCODE, 6) as Ticker, 1 as SuspensionFlag
                from wind.ASHARETRADINGSUSPENSION
                where S_DQ_SUSPENDDATE>='{0}' and S_DQ_SUSPENDDATE<='{1}'
                and S_DQ_SUSPENDTYPE in ('444003000', '444016000')

                union

                select cast(S_DQ_SUSPENDDATE as signed) as Date, left(S_INFO_WINDCODE, 6) as Ticker, 1 as SuspensionFlag
                from wind.ASHARETRADINGSUSPENSION
                where S_DQ_SUSPENDDATE>='{0}' and S_DQ_SUSPENDDATE<='{1}'
                and S_DQ_SUSPENDTYPE not in ('444003000', '444016000')
                and str_to_date(concat(S_DQ_SUSPENDDATE,' 09:30:00'), '%Y%m%d %h:%i')>OPDATE