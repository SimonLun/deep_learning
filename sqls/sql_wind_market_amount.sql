select cast(TRADE_DT as signed) as Date, LEFT(S_INFO_WINDCODE, 6) as Ticker, S_DQ_AMOUNT/10 as amount
        from wind.ASHAREEODPRICES where TRADE_DT>="{0}" and TRADE_DT<="{1}"