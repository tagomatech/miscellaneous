# futuresrollfactory.py
# tagomatech (Nov-22)
# Heavily based on Marcos Lopez de Prado SNIPPET 2.2 FORM A GAPS SERIES, DETRACT IT FROM PRICES
# in Advancces in Financial Machine Learning


import pandas as pd

def compute_roll_gaps(df:pd.DataFrame, matchEnd=True) -> pd.DataFrame:
    
    # Compute gaps at each roll, between previous close and next open
    roll_dates = df['Instrument'].drop_duplicates(keep='first').index
    
    gaps = df['Close'] * 0
    
    iloc = list(df.index)
    iloc = [iloc.index(i) - 1 for i in roll_dates] # index of days prior to roll
    
    gaps.loc[roll_dates[1: ]] = df['Open'].loc[roll_dates[1: ]] - df['Close'].iloc[iloc[1: ]].values
    gaps = gaps.cumsum()
    
    if matchEnd:
        gaps -= gaps.iloc[-1] # roll backward
    
    return gaps


def get_rolled_futures(df:pd.DataFrame) -> pd.DataFrame:
    
    # Rename original BBG columns
    dic_cols = {'FUT_CUR_GEN_TICKER' : 'Instrument',
                            'PX_OPEN' : 'Open',
                            'PX_HIGH' : 'High',
                            'PX_LOW' : 'Low',
                            'PX_LAST' : 'Last',
                            'PX_SETTLE' : 'Close',
                            'EQY_WEIGHTED_AVG_PX' : 'VWAP'}
    df = df.rename(columns=dic_cols)

    gaps = compute_roll_gaps(df)

    for fld in ['Open', 'High', 'Low', 'Last', 'Close', 'VWAP']:
        df[fld] -= gaps
    
    return df
