para = {'parquet_path': "C:/Users/LYQ/Desktop/parquet_data/",
        'start_date_list': ['20130101', '20140101', '20150101', '20160101', '20170101', '20180101',
                            '20190101',
                            '20200101', '20210101', '20220101'],
        'end_date_list': ['20140101', '20150101', '20160101', '20170101', '20180101', '20190101',
                          '20200101',
                          '20210101', '20220101', '20230101'],
        'single_factor_report_path':'C:/Users/LYQ/Desktop/single_factor_report/',
        'factor_npy_path': 'C:/Users/LYQ/Desktop/factor_npy/',

        'base_index': ['000016.SH', '000300.SH', '000905.SH', '000906.SH', '000852.SH'],
        'base_hash': ['SZ50', 'HS300', 'ZZ500', 'ZZ800', 'ZZ1000'],
        'exchange_code': {'XSHE': 'SZ', 'XSHG': 'SH', 'XBEI': 'BJ', 'SSE': 'SH', 'SZSE': 'SZ', 'BSE': 'BJ'},
        'industry_index_1': ['CI005001.WI', 'CI005002.WI', 'CI005003.WI', 'CI005004.WI',
                             'CI005005.WI', 'CI005006.WI', 'CI005007.WI', 'CI005008.WI',
                             'CI005009.WI', 'CI005010.WI', 'CI005011.WI', 'CI005012.WI',
                             'CI005013.WI', 'CI005014.WI', 'CI005015.WI', 'CI005016.WI',
                             'CI005017.WI', 'CI005018.WI', 'CI005019.WI', 'CI005020.WI',
                             'CI005021.WI', 'CI005022.WI', 'CI005023.WI', 'CI005024.WI',
                             'CI005025.WI', 'CI005026.WI', 'CI005027.WI', 'CI005028.WI',
                             'CI005029.WI', 'CI005030.WI'],
        'industry_dict': {"01":"石油石化", "02":"煤炭", "03":"有色金属", "04":"电力及公用事业",
                                  "05":"钢铁", "06":"基础化工", "07":"建筑", "08":"建材",
                                  "09":"轻工制造", "10":"机械", "11":"电力设备及新能源", "12":"国防军工",
                                  "13":"汽车", "14":"商贸零售", "15":"消费者服务", "16":"家电",
                                  "17":"纺织服装", "18":"医药", "19":"食品饮料", "20":"农林牧渔",
                                  "21":"银行", "22":"非银行金融", "23":"房地产", "24":"交通运输",
                                  "25":"电子", "26":"通信", "27":"计算机", "28":"传媒",
                                  "29":"综合", "30":"综合金融"},
        'industry_index_1_hash': ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14",
                                  "15",
                                  "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
                                  "30"],
        
        'factor_category': ['value_factor_list', 'growth_factor_list', 'financial_factor_list', 'psi_factor_list',
                            'momentum_factor_list', 'volatility_factor_list', 'turnover_factor_list',
                            'sentiment_factor_list', 'technical_factor_list', 'expectation_factor_list',
                            'moneyflow_factor_list'],
        'value_factor_list': ['CTOP', 'CTP5', 'ETOP', 'ETP5', 'LCAP', 'LFLO', 'PB', 'PCF', 'PE', 'PS', 'TA2EV', 'PEG3Y',
                              'PEG5Y', 'PEIndu', 'PBIndu', 'PSIndu', 'PCFIndu', 'StaticPE', 'ForwardPE', 'MktValue',
                              'NegMktValue', 'EPIBS', 'CETOP', 'NLSIZE'],
        'growth_factor_list': ['EGRO', 'FinancingCashGrowRate',
                               'InvestCashGrowRate', 'NetAssetGrowRate', 'NetProfitGrowRate', 'NPParentCompanyGrowRate',
                               'OperatingProfitGrowRate', 'OperatingRevenueGrowRate', 'OperCashGrowRate', 'SUE',
                               'TotalAssetGrowRate', 'TotalProfitGrowRate', 'FEARNG', 'FSALESG', 'SUOI',
                               'NetProfitGrowRate3Y', 'NetProfitGrowRate5Y', 'OperatingRevenueGrowRate3Y',
                               'OperatingRevenueGrowRate5Y', 'NetCashFlowGrowRate', 'NPParentCompanyCutYOY', 'SGRO',
                               'EgibsLong'],
        'financial_factor_list': ['ASSI', 'TotalAssets', 'TEAP', 'NIAP', 'TotalFixedAssets', 'IntFreeCL', 'IntFreeNCL',
                                  'IntCL', 'IntDebt', 'NetDebt', 'NetTangibleAssets', 'WorkingCapital',
                                  'NetWorkingCapital', 'TotalPaidinCapital', 'RetainedEarnings', 'OperateNetIncome',
                                  'ValueChgProfit', 'NetIntExpense', 'EBIT', 'EBITDA', 'EBIAT', 'NRProfitLoss',
                                  'NIAPCut', 'FCFF', 'FCFE', 'DA', 'TRevenueTTM', 'TCostTTM', 'RevenueTTM', 'CostTTM',
                                  'GrossProfitTTM', 'SalesExpenseTTM', 'AdminExpenseTTM', 'FinanExpenseTTM',
                                  'AssetImpairLossTTM', 'NPFromOperatingTTM', 'NPFromValueChgTTM', 'OperateProfitTTM',
                                  'NonOperatingNPTTM', 'TProfitTTM', 'NetProfitTTM', 'NetProfitAPTTM',
                                  'SaleServiceRenderCashTTM', 'NetOperateCFTTM', 'NetInvestCFTTM', 'NetFinanceCFTTM',
                                  'GrossProfit', 'AccountsPayablesTDays', 'AccountsPayablesTRate', 'AdminiExpenseRate',
                                  'ARTDays', 'ARTRate', 'BLEV', 'BondsPayableToAsset', 'CashRateOfSales',
                                  'CashToCurrentLiability', 'CurrentAssetsRatio', 'CurrentAssetsTRate', 'CurrentRatio',
                                  'DebtEquityRatio', 'DebtsAssetRatio', 'EBITToTOR', 'EquityFixedAssetRatio',
                                  'EquityToAsset', 'EquityTRate', 'FinancialExpenseRate', 'FixAssetRatio',
                                  'FixedAssetsTRate', 'GrossIncomeRatio', 'IntangibleAssetRatio', 'InventoryTDays',
                                  'InventoryTRate', 'LongDebtToAsset', 'LongDebtToWorkingCapital',
                                  'LongTermDebtToAsset', 'MLEV', 'NetProfitRatio', 'NOCFToOperatingNI',
                                  'NonCurrentAssetsRatio', 'NPToTOR', 'OperatingExpenseRate', 'OperatingProfitRatio',
                                  'OperatingProfitToTOR', 'OperCashInToCurrentLiability', 'QuickRatio', 'ROA', 'ROA5',
                                  'ROE', 'ROE5', 'SalesCostRatio', 'SaleServiceCashToOR', 'TaxRatio',
                                  'TotalAssetsTRate',
                                  'TotalProfitCostRatio', 'CFO2EV', 'ACCA', 'DEGM', 'CashDividendCover',
                                  'DividendCover', 'DividendPaidRatio', 'RetainedEarningRatio', 'NetNonOIToTP',
                                  'NetNonOIToTPLatest',
                                  'PeriodCostsRate', 'InterestCover', 'NetProfitCashCover', 'OperCashInToAsset',
                                  'CashConversionCycle', 'OperatingCycle',
                                  'ROEDiluted', 'ROEAvg', 'ROEWeighted', 'ROECut', 'ROECutWeighted',
                                  'ROIC', 'ROAEBIT', 'ROAEBITTTM', 'OperatingNIToTP', 'OperatingNIToTPLatest',
                                  'InvestRAssociatesToTP', 'InvestRAssociatesToTPLatest', 'NPCutToNP',
                                  'SuperQuickRatio', 'TSEPToInterestBearDebt', 'DebtTangibleEquityRatio',
                                  'TangibleAToInteBearDebt', 'TangibleAToNetDebt', 'NOCFToTLiability',
                                  'NOCFToInterestBearDebt', 'NOCFToNetDebt', 'TSEPToTotalCapital',
                                  'InteBearDebtToTotalCapital', 'SalesServiceCashToORLatest', 'CashRateOfSalesLatest',
                                  'NOCFToOperatingNILatest'],
        'psi_factor_list': ['DilutedEPS', 'EPS', 'CashEquivalentPS', 'DividendPS', 'EPSTTM', 'NetAssetPS', 'TORPS',
                            'TORPSLatest', 'OperatingRevenuePS', 'OperatingRevenuePSLatest', 'OperatingProfitPS',
                            'OperatingProfitPSLatest', 'CapitalSurplusFundPS', 'SurplusReserveFundPS',
                            'UndividedProfitPS', 'RetainedEarningsPS', 'OperCashFlowPS', 'CashFlowPS',
                            'EnterpriseFCFPS', 'ShareholderFCFPS'],
        'momentum_factor_list': ['REVS10', 'REVS20', 'REVS5', 'RSTR12', 'RSTR24', 'EARNMOM', 'FiftyTwoWeekHigh',
                                 'BIAS10', 'BIAS20', 'BIAS5', 'BIAS60', 'CCI10', 'CCI20', 'CCI5', 'CCI88', 'ROC6',
                                 'ROC20', 'SRMI', 'ChandeSD', 'ChandeSU', 'CMO', 'ARC', 'AD', 'AD20', 'AD6',
                                 'CoppockCurve', 'Aroon', 'AroonDown', 'AroonUp', 'DEA', 'DIFF', 'DDI', 'DIZ', 'DIF',
                                 'PVT', 'PVT6', 'PVT12', 'TRIX5', 'TRIX10', 'MA10RegressCoeff12', 'MA10RegressCoeff6',
                                 'PLRC6', 'PLRC12', 'APBMA', 'BBIC', 'MA10Close', 'BearPower', 'BullPower', 'RC12',
                                 'RC24', 'REVS60', 'REVS120', 'REVS250', 'REVS750', 'REVS5m20', 'REVS5m60',
                                 'REVS5Indu1', 'REVS20Indu1', 'Price1M', 'Price3M', 'Price1Y', 'Rank1M', 'PEHist20',
                                 'PEHist60', 'PEHist120', 'PEHist250', 'RSTR504'],

        'volatility_factor_list': ['CMRA', 'HBETA', 'HSIGMA', 'TOBT', 'Skewness', 'BackwardADJ', 'Variance20',
                                   'Variance60', 'Variance120', 'Kurtosis20', 'Kurtosis60', 'Kurtosis120', 'Alpha20',
                                   'Alpha60', 'Alpha120', 'Beta20', 'Beta60', 'Beta120', 'SharpeRatio20',
                                   'SharpeRatio60', 'SharpeRatio120', 'TreynorRatio20', 'TreynorRatio60',
                                   'TreynorRatio120', 'InformationRatio20', 'InformationRatio60', 'InformationRatio120',
                                   'GainVariance20', 'GainVariance60', 'GainVariance120', 'LossVariance20',
                                   'LossVariance60', 'LossVariance120', 'GainLossVarianceRatio20',
                                   'GainLossVarianceRatio60', 'GainLossVarianceRatio120', 'RealizedVolatility',
                                   'Beta252', 'DASTD', 'CmraCNE5', 'HsigmaCNE5', 'DDNBT', 'DDNCR', 'DDNSR', 'DVRAT'],

        'turnover_factor_list': ['DAVOL10', 'DAVOL20', 'DAVOL5', 'VOL10', 'VOL120', 'VOL20', 'VOL240', 'VOL5', 'VOL60',
                                 'Volatility', 'STOM', 'STOQ', 'STOA'],
        'sentiment_factor_list': ['DAREC', 'GREC', 'REC', 'MAWVAD', 'PSY', 'RSI', 'WVAD', 'ADTM', 'ATR14', 'ATR6',
                                  'SBM', 'STM', 'OBV', 'OBV6', 'OBV20', 'TVMA20', 'TVMA6', 'TVSTD20', 'TVSTD6', 'VDEA',
                                  'VDIFF', 'VEMA10', 'VEMA12', 'VEMA26', 'VEMA5', 'VMACD', 'VOSC', 'VR', 'VROC12',
                                  'VROC6', 'VSTD10', 'VSTD20', 'KlingerOscillator', 'MoneyFlow20', 'ACD6', 'ACD20',
                                  'AR', 'BR', 'ARBR', 'NVI', 'PVI', 'JDQS20', 'Volumn1M', 'Volumn3M'],
        'technical_factor_list': ['DBCD', 'DHILO', 'EMA10', 'EMA120', 'EMA20', 'EMA5', 'EMA60', 'MA10', 'MA120', 'MA20',
                                  'MA5', 'MA60', 'MFI', 'ILLIQUIDITY', 'MACD', 'BollDown', 'BollUp', 'KDJ_K', 'KDJ_D',
                                  'KDJ_J', 'UpRVI', 'DownRVI', 'RVI', 'ASI', 'ChaikinOscillator', 'ChaikinVolatility',
                                  'EMV14', 'EMV6', 'plusDI', 'minusDI', 'ADX', 'ADXR', 'MTM', 'MTMMA', 'UOS',
                                  'SwingIndex', 'Ulcer10', 'Ulcer5', 'Hurst', 'EMA12', 'EMA26', 'BBI', 'TEMA10',
                                  'TEMA5', 'CR20', 'MassIndex', 'Elder'],
        'expectation_factor_list': ['FY12P',
                                    'DAREV',
                                    'GREV',
                                    'SFY12P',
                                    'DASREV',
                                    'GSREV',
                                    's_west_netprofit_ftm_chg_1m',
                                    's_west_netprofit_ftm_chg_3m',
                                    's_west_netprofit_ftm_chg_6m',
                                    's_west_eps_ftm_chg_1m',
                                    's_west_eps_ftm_chg_3m',
                                    's_west_eps_ftm_chg_6m',
                                    'S_WEST_SALES_FTM_CHG_1M',
                                    's_west_sales_ftm_chg_3m',
                                    's_west_sales_ftm_chg_6m',
                                    's_west_netprofit_fy1_chg_1m',
                                    's_west_netprofit_fy1_chg_3m',
                                    's_west_netprofit_fy1_chg_6m',
                                    's_west_eps_fy1_chg_1m',
                                    's_west_eps_fy1_chg_3m',
                                    's_west_eps_fy1_chg_6m',
                                    'S_WEST_SALES_FY1_CHG_1M',
                                    's_west_sales_fy1_chg_3m',
                                    's_west_sales_fy1_chg_6m',
                                    's_west_netprofit_ftm_1m',
                                    's_west_netprofit_ftm_3m',
                                    's_west_netprofit_ftm_6m',
                                    's_west_eps_ftm_1m',
                                    's_west_eps_ftm_3m',
                                    's_west_eps_ftm_6m',
                                    's_west_sales_ftm_1m',
                                    's_west_sales_ftm_3m',
                                    # 's_west_sales_ftm_6m',
                                    's_west_netprofit_fy1_1m',
                                    's_west_netprofit_fy1_3m',
                                    's_west_netprofit_fy1_6m',
                                    's_west_eps_fy1_1m',
                                    's_west_eps_fy1_3m',
                                    's_west_eps_fy1_6m',
                                    's_west_sales_fy1_1m',
                                    's_west_sales_fy1_3m',
                                    's_west_sales_fy1_6m',
                                    's_west_eps_fygrowth',
                                    's_west_netprofitmaxmin_fy1',
                                    's_west_epsmaxmin_fy1',
                                    's_west_salesmaxmin_fy1',
                                    's_west_stdnetprofit_fy1',
                                    's_west_stdeps_fy1',
                                    's_west_stdsales_fy1'],
        'moneyflow_factor_list': ['NET_INFLOW_RATE_VOLUME', 'S_MFD_INFLOW_OPENVOLUME', 'OPEN_NET_INFLOW_RATE_VOLUME',
                                  'S_MFD_INFLOW_CLOSEVOLUME', 'CLOSE_NET_INFLOW_RATE_VOLUME', 'S_MFD_INFLOW',
                                  'NET_INFLOW_RATE_VALUE', 'S_MFD_INFLOW_OPEN', 'OPEN_NET_INFLOW_RATE_VALUE',
                                  'S_MFD_INFLOW_CLOSE', 'CLOSE_NET_INFLOW_RATE_VALUE', 'TOT_VOLUME_BID',
                                  'TOT_VOLUME_ASK', 'MONEYFLOW_PCT_VOLUME', 'OPEN_MONEYFLOW_PCT_VOLUME',
                                  'CLOSE_MONEYFLOW_PCT_VOLUME', 'MONEYFLOW_PCT_VALUE', 'OPEN_MONEYFLOW_PCT_VALUE',
                                  'CLOSE_MONEYFLOW_PCT_VALUE', 'S_MFD_INFLOWVOLUME_LARGE_ORDER',
                                  'NET_INFLOW_RATE_VOLUME_L', 'S_MFD_INFLOW_LARGE_ORDER', 'NET_INFLOW_RATE_VALUE_L',
                                  'MONEYFLOW_PCT_VOLUME_L', 'MONEYFLOW_PCT_VALUE_L', 'S_MFD_INFLOW_OPENVOLUME_L',
                                  'OPEN_NET_INFLOW_RATE_VOLUME_L', 'S_MFD_INFLOW_OPEN_LARGE_ORDER',
                                  'OPEN_NET_INFLOW_RATE_VALUE_L', 'OPEN_MONEYFLOW_PCT_VOLUME_L',
                                  'OPEN_MONEYFLOW_PCT_VALUE_L', 'S_MFD_INFLOW_CLOSEVOLUME_L',
                                  'CLOSE_NET_INFLOW_RATE_VOLUME_L', 'S_MFD_INFLOW_CLOSE_LARGE_ORDER',
                                  'CLOSE_NET_INFLOW_RATE_VALU_L', 'CLOSE_MONEYFLOW_PCT_VOLUME_L',
                                  'CLOSE_MONEYFLOW_PCT_VALUE_L', 'BUY_VALUE_EXLARGE_ORDER_ACT',
                                  'SELL_VALUE_EXLARGE_ORDER_ACT', 'BUY_VALUE_LARGE_ORDER_ACT',
                                  'SELL_VALUE_LARGE_ORDER_ACT', 'BUY_VALUE_MED_ORDER_ACT', 'SELL_VALUE_MED_ORDER_ACT',
                                  'BUY_VALUE_SMALL_ORDER_ACT', 'SELL_VALUE_SMALL_ORDER_ACT',
                                  'BUY_VOLUME_EXLARGE_ORDER_ACT',
                                  'SELL_VOLUME_EXLARGE_ORDER_ACT',
                                  'BUY_VOLUME_LARGE_ORDER_ACT',
                                  'SELL_VOLUME_LARGE_ORDER_ACT',
                                  'BUY_VOLUME_MED_ORDER_ACT',
                                  'SELL_VOLUME_MED_ORDER_ACT',
                                  'BUY_VOLUME_SMALL_ORDER_ACT',
                                  'SELL_VOLUME_SMALL_ORDER_ACT'],
        'final_factor_list':['CTP5',
'ETOP',
'TA2EV',
'GrossProfit',
'CFO2EV',
'AdminExpenseTTM',
'NIAP',
'TEAP',
'OperCashFlowPS',
'REVS5m60',
'REVS5m20',
'BBIC',
'Beta20',
'DDNCR',
'ILLIQUIDITY',
'minusDI',
'FY12P',
'OPEN_MONEYFLOW_PCT_VALUE',
'S_MFD_INFLOW_OPENVOLUME_L',
'S_MFD_INFLOW_OPEN_LARGE_ORDER']
	
        }
