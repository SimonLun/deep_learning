select S_INFO_CODE as ticker, cast(S_INFO_LISTDATE as signed) as ipo_date
                from wind.ASHAREDESCRIPTION
                where S_INFO_LISTDATE IS NOT NULL
                and NOT ((S_INFO_LISTDATE<'{0}') OR (S_INFO_LISTDATE>'{1}'))