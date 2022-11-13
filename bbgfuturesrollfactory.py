# bbgfuturesrollfactory.py
# tagoma (Nov.22)


import datetime as dt
import pandas as pd
from blp import blp


class BBGFuturesRollFactory:
    """
    Essentially it is a wrapper around Marcos Lopez de Prado's snippet codes
    SNIPPET 2.2 FORM A GAPS SERIES, DETRACT IT FROM PRICES and
    SNIPPER 2.3 NON-NEGATIVE ROLLED PRICE SERIES
    from his book Advancces in Financial Machine Learning

    Example:
    -------
    >>>bbg_conn = BBGFuturesRollFactory()
    >>>bbg_conn.start()
    >>>bbg_data = bbg_conn.get_data(ticker='IJ1 Comdty',
                                    start_date='20121113',
                                    end_date='20221111',
                                    lst_fields=['PX_OPEN', 'PX_HIGH', 'PX_LOW',
                                                'PX_LAST','PX_SETTLE', 'EQY_WEIGHTED_AVG_PX'])
    >>>bbg_conn.stop()
    >>>rolled_futures = bbg_conn.roll(bbg_data)
    """

    def __init__(self) -> None:

        self.bquery = blp.BlpQuery()


    def start(self) -> None:

        self.bquery.start()


    def stop(self) -> None:

        self.bquery.stop()


    def get_data(self, ticker:str, start_date:str, end_date:str, lst_fields:list) -> pd.DataFrame:

        lst_fields = list(set(lst_fields + ['PX_OPEN', 'PX_LAST', 'FUT_CUR_GEN_TICKER'])) # As these 3 fields is not already in the list

        return  self.bquery.bdh(
            ticker,
            lst_fields,
            start_date=start_date,
            end_date=end_date,
        )


    def roll(self, bbg_data:pd.DataFrame, cols_to_not_roll:list=[], roll_backward=True) -> pd.DataFrame:

        cols_to_not_roll = list(set(cols_to_not_roll + ['date', 'security', 'FUT_CUR_GEN_TICKER'])) # As these 3 fields is not already in the list
        cols_to_roll = set(bbg_data.columns.tolist()) - set(cols_to_not_roll)

        bbg_data = bbg_data.copy(deep=True)

        df_rolled = pd.DataFrame()
        df_rolled = bbg_data.assign(date = bbg_data.date.values)

        gaps = self.compute_roll_gaps(df=bbg_data, roll_backward=roll_backward)

        # Stabdard roll
        for col in cols_to_roll:
            df_rolled[col] -= gaps

        # Non-negative rolled price series
        for col in cols_to_roll:
            df_rolled = df_rolled.assign(**{'calc_ret' + col: df_rolled[col].diff() / bbg_data[col].shift(1)})
            df_rolled = df_rolled.assign(**{ 'r' + col:  (1 + df_rolled['calc_ret' + col]).cumprod()})

        # Drop columns with name containing 'calc_ret'
        df_rolled = df_rolled.filter(regex='^((?!calc_ret).)*$')

        return df_rolled


    @staticmethod
    def compute_roll_gaps(df:pd.DataFrame, roll_backward=True) -> pd.DataFrame:

        # Compute gaps at each roll, between previous close and next open
        roll_dates = df['FUT_CUR_GEN_TICKER'].drop_duplicates(keep='first').index
        
        gaps = df['PX_LAST'] * 0
        
        iloc = list(df.index)
        iloc = [iloc.index(i) - 1 for i in roll_dates] # index of days prior to roll
        
        gaps.loc[roll_dates[1: ]] = df['PX_OPEN'].loc[roll_dates[1: ]] - df['PX_LAST'].iloc[iloc[1: ]].values
        gaps = gaps.cumsum()
        
        if roll_backward:
            gaps -= gaps.iloc[-1] # roll backward
        
        return gaps
