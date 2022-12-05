# bbgtermstucturefactory.py
# etallent Dec-22
# WARNING!  early version; note-to-self; code even not cleaned, ugly!!!

from blp import blp
import datetime as dt
import numpy as np
import pandas as pd
import re


class BBGTermStructureFactory:

    def __init__(self) -> None:
        pass

      
    def get_bbg_data(self, ticker:str, field:str='PX_LAST', continuous_number:int=10, time_window:int=10) -> None:

        """
        field: would typically be 'PX_LAST'
        continuous_number: number of continous contracts; be careful not to go over max number available,
                            e.g. Matif OSR continuous futures has only max 10 futures contracts trading
                            at a time, and this might even change depending on the time/day of the year
        time_window: number time window in years
        """
        
        df_bbg_raw = pd.DataFrame()

        # Pull data from BBG
        bquery = blp.BlpQuery().start() # Start pulling data from BBG
        now = dt.datetime.now()
        end_date_dt = dt.date(now.year, now.month, now.day)
        start_year = now.year - time_window
        start_date_dt = dt.date(start_year, now.month, now.day)

    
        if len(ticker) == 1: ticker += ' '
        lst_tickers = [ticker + str(x) + ' Comdty' for x in range(1, continuous_number + 1)]

        df_bbg_data = bquery.bdh(
            lst_tickers,
            [field, 'FUT_CUR_GEN_TICKER'],
            start_date=start_date_dt.strftime('%Y%m%d'),
            end_date=end_date_dt.strftime('%Y%m%d'),
        )

        bquery.stop()

        return df_bbg_data
    

    def bbg_data_curve_get(self, df_bbg_data) -> pd.DataFrame:

        dic_years = {'00' : '2000',
                    '01' : '2001',
                    '02' : '2002',
                    '03' : '2003',
                    '04' : '2004',
                    '05' : '2005',
                    '06' : '2006',
                    '07' : '2007',
                    '08' : '2008',
                    '09' : '2009',
                    '10' : '2010',
                    '11' : '2011',
                    '12' : '2012',
                    '13' : '2013',
                    '14' : '2014',
                    '15' : '2015',
                    '16' : '2016',
                    '17' : '2017',
                    '18' : '2018',
                    '19' : '2019',
                    '20' : '2020',
                    '21' : '2021',
                    '2' : '2022',
                    '3' : '2023',
                    '4' : '2024',
                    '5' : '2025',
                    '6' : '2026'
                    }

        dic_months = {'F' : 1,
                    'G' : 2,
                    'H' : 3,
                    'J' : 4,
                    'K' : 5,
                    'M' : 6,
                    'N' : 7,
                    'Q' : 8,
                    'U' : 9,
                    'V' : 10,
                    'X' : 11,
                    'Z' : 12,
                    }


        def dic_to_dataframe(dic:dict, col_name:str) -> pd.DataFrame:
            
            col_name_in = col_name + '_in'
            col_name_out = col_name + '_out'
            
            dic_final = {col_name_in : [_ for _ in dic.keys()],#[dic[k] for k in dic],
                        col_name_out : [_ for _ in dic.values()]}
            
            return pd.DataFrame.from_dict(dic_final)

        df_years = dic_to_dataframe(dic_years, 'year')
        df_months = dic_to_dataframe(dic_months, 'month')

        
        def dataframe_cleaner(df:pd.DataFrame) -> pd.DataFrame: # Wrote as a function in case other required cleansing operations emerge in the future
    
            df = df.dropna()
            lst_datetimes = df['date'].values
            lst_dates = [pd.to_datetime(x).date() for x in lst_datetimes]
            df = df.assign(date = lst_dates)

            df = df.rename(columns={df.columns.to_list()[2] : 'price'})

            return df

        
        df_transfo = dataframe_cleaner(df_bbg_data) # Argumentwas df_transfo in notebook
        df_transfo = df_transfo.assign(ticker = df_bbg_data.FUT_CUR_GEN_TICKER.apply(lambda x: re.findall('^\w{1,2}', x)[0]))
        df_transfo = df_transfo.assign(continuous_nb = df_transfo.security.apply(lambda x: re.findall('(?=\w+)\d+(?<!\s)', x)[0]))
        df_transfo.continuous_nb = df_transfo.continuous_nb.astype(int)
        df_transfo = df_transfo.assign(short_year =  df_transfo.FUT_CUR_GEN_TICKER.apply(lambda x: re.findall('\d+', x)[0]))
        df_transfo = df_transfo.assign(short_month =  df_transfo.FUT_CUR_GEN_TICKER.apply(lambda x: re.findall('(\w)(?:\d+)', x)[0]))

        df_transfo = pd.merge(df_transfo, df_years, left_on='short_year', right_on='year_in')

        df_transfo = pd.merge(df_transfo, df_months, left_on='short_month', right_on='month_in')
        
        return df_transfo

        
    def bbg_data_curve_at_ref_date(self, df:pd.DataFrame, ref_date_dt:dt.date=dt.datetime.today().date()) -> pd.DataFrame:
        

        #df_curve_ref_date = df[(pd.to_datetime(df.date).dt.date == ref_date_dt)].sort_values(['year_out', 'month_out'])
        df_ref_date = df[df['date'] == ref_date_dt].sort_values(['year_out', 'month_out'])

        # Find out the month maturity for the 1st continuous at reference date
        leading_month = df_ref_date.short_month.head(1).values[0]

        # Extract unique dates where continuous_nb == 1 and short_month == leading_month
        df_unique_date = df.loc[(df.continuous_nb == 1) & (df.short_month == leading_month)].sort_values(['date', 'month_in'])

        # Now make the list of all dates
        lst_dates_left = df_unique_date['date'].unique().tolist()

        df_final = pd.DataFrame()

        for d in lst_dates_left:
            #print(d, '\n')
            df_curve_ref_date = df[df['date'] == d].sort_values(['year_out', 'month_out'])
            #print(df_curve_ref_date)

            lst_months_unique = df_curve_ref_date.month_in.unique().tolist()
            df_curve_ref_date.assign(xaxis = np.nan)

            for month in lst_months_unique:
                lst_cnt = list(range(1, df_curve_ref_date[df_curve_ref_date.month_in == month].shape[0] + 1))
                df_curve_ref_date.loc[df.month_in == month , 'df_curve_ref_date'] =  [month + ' (' + str(x) + ')' for x in lst_cnt]

            df_final = pd.concat([df_final, df_curve_ref_date])
            
        lst_columns_unpiv = df_final.iloc[ : , 0:12].columns.tolist()
        df_final = pd.melt(df_final, id_vars=lst_columns_unpiv, var_name='col_to_drop', value_name='xaxis_label')#columns='df_curve_ref_date')
        df_final = df_final.drop(columns='col_to_drop')

        return df_final
