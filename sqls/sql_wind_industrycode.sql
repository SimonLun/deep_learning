select {0} as Date, left (S_CON_WINDCODE, 6) as Ticker, S_INFO_WINDCODE as IndustryCode
from wind.AINDEXMEMBERSCITICS
where {0} between S_CON_INDATE and ifnull(S_CON_OUTDATE, '21000101')