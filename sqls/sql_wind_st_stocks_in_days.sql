select left(S_INFO_WINDCODE,6) as Ticker, S_TYPE_ST,
                cast(ENTRY_DT as signed) as ENTRY_DT,
                case when REMOVE_DT is not null then cast(REMOVE_DT as signed) else 21000101 end as REMOVE_DT
                from wind.ASHAREST where NOT (ifnull(REMOVE_DT, '21000101')<"{0}" OR ENTRY_DT>"{1}")