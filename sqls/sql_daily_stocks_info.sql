select S_INFO_CODE as ticker, S_INFO_NAME as company_name, S_INFO_COMPNAME as full_company_name,
cast(S_INFO_LISTDATE as signed) as ipo_date,
case when S_INFO_DELISTDATE IS NULL THEN S_INFO_DELISTDATE else cast(S_INFO_DELISTDATE as signed) end as delist_date,
S_INFO_EXCHMARKET as exchange, S_INFO_LISTBOARDNAME as board_name, IS_SHSC
from wind.ASHAREDESCRIPTION
where S_INFO_LISTDATE IS NOT NULL;
