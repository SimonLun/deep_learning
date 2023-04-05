select * from wind.ASHAREDESCRIPTION where S_INFO_COMPCODE in (
	select S_INFO_COMPCODE from (
		select S_INFO_COMPCODE, count(*) as cn from wind.ASHAREDESCRIPTION where S_INFO_LISTDATE is not null group by  S_INFO_COMPCODE) A
	WHERE cn>1)
    order by S_INFO_COMPCODE