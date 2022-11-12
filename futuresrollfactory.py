# futuresrollfactory.py
# tagomatech (Nov-22)
# Inspired by Marcos Lopez de Prado#s Advancces in Financial Machine Learning
# SNIPPET 2.2 FORM A GAPS SERIES, DETRACT IT FROM PRICES


import pandas as pd


def compute_roll_gaps(df:pd.DataFrame, matchEnd=True) -> pd.DataFrame:
    
    # Compute gaps at each roll, between previous close and next open
    roll_dates = df['FUT_CUR_GEN_TICKER'].drop_duplicates(keep='first').index
    
    gaps = df['PX_LAST'] * 0
    
    iloc = list(df.index)
    iloc = [iloc.index(i) - 1 for i in roll_dates] # index of days prior to roll
    
    gaps.loc[roll_dates[1: ]] = df['PX_OPEN'].loc[roll_dates[1: ]] - df['PX_LAST'].iloc[iloc[1: ]].values
    gaps = gaps.cumsum()
    
    if matchEnd:
        gaps -= gaps.iloc[-1] # roll backward
    
    return gaps


def get_rolled_futures(df:pd.DataFrame) -> pd.DataFrame:
    
    df= df.copy(deep=True)

    gaps = compute_roll_gaps(df)

    for fld in ['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST', 'PX_SETTLE', 'EQY_WEIGHTED_AVG_PX']:
        df[fld] -= gaps
    
    return df
