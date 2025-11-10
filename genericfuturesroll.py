# genericfuturesroll.py
# tagomatech Nov-25

from __future__ import annotations
from typing import Optional, Mapping, Literal, Sequence
import pandas as pd
import numpy as np

# ---------- helpers

_SYNONYMS = {
    "date": ["date", "datetime", "dt", "time"],
    "symbol": ["symbol", "ticker", "contract", "ric", "secid", "security"],
    "open": ["open", "px_open", "o"],
    "high": ["high", "px_high", "h"],
    "low":  ["low", "px_low", "l"],
    "last": ["last", "close", "px_last", "settle", "settlement", "adj_close"],
    "volume": ["volume", "vol", "px_volume", "qty"],
    "openinterest": ["openinterest", "open_interest", "oi"],
    "expiry": ["expiry", "expiration", "maturity", "exp_date"],
}

def _lower_map(cols: Sequence[str]) -> Mapping[str, str]:
    """Map lowercase->original to keep original spelling when renaming."""
    return {c.lower(): c for c in cols}

def _guess(existing: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    """Return the first existing column (original case) that matches any candidate (case-insensitive)."""
    low2orig = _lower_map(existing)
    for cand in candidates:
        c = cand.lower()
        if c in low2orig:
            return low2orig[c]
    return None

def _standardize_columns(
    df: pd.DataFrame,
    *,
    rename_map: Optional[Mapping[str, str]] = None,
    prefer_last_candidates: Sequence[str] = ("last", "close", "settle", "px_last", "adj_close"),
) -> pd.DataFrame:
    """
    Return a copy of df with standardized columns:
    date, symbol, open?, high?, low?, last, volume?, openinterest?, expiry?
    """
    df = df.copy()
    # user overrides first
    if rename_map:
        df = df.rename(columns=rename_map)

    cols = list(df.columns)

    # required
    date_col = _guess(cols, _SYNONYMS["date"])
    sym_col  = _guess(cols, _SYNONYMS["symbol"])
    last_col = _guess(cols, list(prefer_last_candidates))  # explicit preference order

    if not date_col:
        raise KeyError("Could not find a date column (tried: %s)" % _SYNONYMS["date"])
    if not sym_col:
        raise KeyError("Could not find a symbol/ticker column (tried: %s)" % _SYNONYMS["symbol"])
    if not last_col:
        raise KeyError("Could not find a close/last column (tried: %s)" % (list(prefer_last_candidates),))

    # optional
    open_col  = _guess(cols, _SYNONYMS["open"])
    high_col  = _guess(cols, _SYNONYMS["high"])
    low_col   = _guess(cols, _SYNONYMS["low"])
    vol_col   = _guess(cols, _SYNONYMS["volume"])
    oi_col    = _guess(cols, _SYNONYMS["openinterest"])
    exp_col   = _guess(cols, _SYNONYMS["expiry"])

    # build rename dict to standardized names
    rename = {
        date_col: "date",
        sym_col: "symbol",
        last_col: "last",
    }
    if open_col: rename[open_col] = "open"
    if high_col: rename[high_col] = "high"
    if low_col:  rename[low_col]  = "low"
    if vol_col:  rename[vol_col]  = "volume"
    if oi_col:   rename[oi_col]   = "openinterest"
    if exp_col:  rename[exp_col]  = "expiry"

    df = df.rename(columns=rename)
    return df

# ---------- main

def build_continuous_futures(
    data: pd.DataFrame,
    *,
    roll_type: Literal["backward", "forward"] = "backward",
    rename_map: Optional[Mapping[str, str]] = None,
    prefer_last_candidates: Sequence[str] = ("last", "close", "settle", "px_last", "adj_close"),
    chain_selector: Literal["as_is", "by_openinterest", "by_volume"] = "as_is",
) -> pd.DataFrame:
    """
    Build a roll-adjusted continuous futures series from a generic pandas DataFrame.

    Expected inputs (any common naming is OK; auto-detected):
      - date column
      - symbol/ticker/contract column -> which specific contract your row belongs to
      - a close/last price column (we'll call it 'last' internally)

    Optional columns (used if present): open, high, low, volume, openinterest, expiry.

    Parameters
    ----------
    data : pd.DataFrame
        Raw futures rows (can include multiple symbols).
        Rows should be daily (or uniform) observations per (date, symbol).
    roll_type : {"backward", "forward"}, default "backward"
        - "backward": anchor to the most recent contract; adjust older history UP.
        - "forward":  anchor to the earliest contract; adjust newer history DOWN.
    rename_map : dict, optional
        Manual column renames applied before auto-detection (e.g. {"Close":"last","Ticker":"symbol"}).
    prefer_last_candidates : sequence of str
        Preference order for picking the close/last column when multiple exist.
    chain_selector : {"as_is","by_openinterest","by_volume"}
        If multiple rows share the same date across different symbols:
          - "as_is": assume data already represents the chosen chain per date.
          - "by_openinterest": within each date, pick the row with max open interest.
          - "by_volume": within each date, pick the row with max volume.

    Returns
    -------
    pd.DataFrame
        Continuous series with columns:
          date, symbol, Contract_ID, Roll_Adjustment,
          [open, high, low, last], and their *_Adj counterparts if present,
          plus any remaining original columns.
    """
    # 1) Standardize column names
    df = _standardize_columns(
        data,
        rename_map=rename_map,
        prefer_last_candidates=prefer_last_candidates,
    )

    # 2) Basic cleaning
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "symbol", "last"]).copy()
    # ensure numeric for prices
    for c in ["open", "high", "low", "last"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 3) If there are multiple symbols per date, optionally select a single "front" row per date
    if chain_selector != "as_is":
        metric = "openinterest" if chain_selector == "by_openinterest" else "volume"
        if metric not in df.columns:
            raise KeyError(
                f"chain_selector='{chain_selector}' requires column '{metric}', which was not found."
            )
        df = (
            df.sort_values(["date"])  # stable order
              .loc[df.groupby("date")[metric].idxmax()]  # pick the symbol with max metric per date
              .sort_values("date")
              .reset_index(drop=True)
        )
    else:
        # If duplicates remain (same date appears with multiple symbols),
        # we assume the user already provided a single chosen contract per date.
        # To be safe, sort (date, symbol) then keep the last occurrence per date.
        # Comment this out if you prefer to raise on duplicates instead.
        dup_mask = df.duplicated(subset=["date"], keep=False)
        if dup_mask.any():
            # Keep the last row per date (user can override with chain_selector)
            df = (df.sort_values(["date", "symbol"])
                    .drop_duplicates(subset=["date"], keep="last")
                    .reset_index(drop=True))

    # 4) Sort by date and compute contract segments when the symbol changes
    df = df.sort_values("date").reset_index(drop=True)
    df["Contract_ID"] = df["symbol"].ne(df["symbol"].shift()).cumsum()

    # 5) Compute roll gaps using 'last'
    grp = df.groupby("Contract_ID", sort=True)["last"]
    last_per_contract = grp.last()
    first_next = grp.first().shift(-1)
    gaps = (first_next - last_per_contract).fillna(0.0)  # Series indexed by Contract_ID

    # Backward = sum of future gaps i..end; Forward = -sum of past gaps up to i-1
    backward_adj = gaps.iloc[::-1].cumsum().iloc[::-1]
    forward_adj  = -(gaps.cumsum().shift(1).fillna(0.0))

    adj_by_contract = backward_adj if roll_type == "backward" else forward_adj
    df["Roll_Adjustment"] = df["Contract_ID"].map(adj_by_contract).astype(float)

    # 6) Apply adjustment to available price columns
    price_cols_order = [c for c in ("high", "low", "open", "last") if c in df.columns]
    for c in price_cols_order:
        df[f"{c}_Adj"] = df[c] + df["Roll_Adjustment"]

    # 7) Reorder columns (readable)
    front = ["date", "symbol", "Contract_ID", "Roll_Adjustment"] + price_cols_order
    front += [f"{c}_Adj" for c in price_cols_order]
    remaining = [c for c in df.columns if c not in front]
    df = df.loc[:, front + remaining].reset_index(drop=True)
    return df
