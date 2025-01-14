import sqlite3
import pandas as pd
import math
from pathlib import Path

# TA library
import ta
from ta.momentum import StochasticOscillator, RSIIndicator, WilliamsRIndicator
from ta.trend import CCIIndicator, MACD, SMAIndicator, EMAIndicator
from ta.volatility import BollingerBands

STOCK_DB_PATH = Path(__file__).parent / "stock_data.db"

def parse_euro_number(val_str):
    """
    Convert a Euro-style string like '2.140,00' to float:
      1) Remove '.' (thousands sep)
      2) Replace ',' with '.'
      3) Convert to float
    Returns None if conversion fails or val_str is empty.
    """
    if val_str in ("", "None", "nan"):
        return None
    step1 = val_str.replace(".", "")
    step2 = step1.replace(",", ".")
    try:
        return float(step2)
    except ValueError:
        return None

def compute_tv_style_signal(buy_count, sell_count):
    """
    Summarizes buy/sell counts into a final signal:
    'Buy' if buys > sells, 'Sell' if sells > buys, else 'Neutral'.
    """
    if buy_count > sell_count:
        return "Buy"
    elif sell_count > buy_count:
        return "Sell"
    else:
        return "Neutral"

def compute_all_indicators_and_aggregate(publisher_code, tf="1D"):
    """
    Main function to query stock_data.db for publisher_code, parse numeric columns,
    compute 10 technical indicators (5 oscillators + 5 moving averages) at short/med/long
    windows, store them in the final row, and return aggregated signals.
    """
    conn = sqlite3.connect(STOCK_DB_PATH)
    query = """
        SELECT date, price, quantity, max, min
        FROM stock_data
        WHERE publisher_code = ?
        ORDER BY date ASC
    """
    df = pd.read_sql_query(query, conn, params=[publisher_code])
    conn.close()

    if df.empty:
        return {
            "publisher": publisher_code,
            "records": [],
            "msg": "No data found",
            "oscSummary": {},
            "maSummary": {},
            "overallSummary": {}
        }

    # Rename columns
    df.rename(columns={
        "price": "close",
        "quantity": "volume",
        "max": "high",
        "min": "low"
    }, inplace=True)

    # Convert date
    df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
    df.sort_values("date", inplace=True, ignore_index=True)

    # Convert numeric columns
    for col in ["close", "high", "low", "volume"]:
        df[col] = df[col].astype(str).apply(parse_euro_number)
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows missing date/close
    df.dropna(subset=["date", "close"], inplace=True)
    if df.empty:
        return {
            "publisher": publisher_code,
            "records": [],
            "msg": "All records invalid.",
            "oscSummary": {},
            "maSummary": {},
            "overallSummary": {}
        }

    short_win = 7
    medium_win = 14
    long_win = 30

    # Build a list of daily records
    records = []
    for _, row in df.iterrows():
        if pd.isna(row["date"]):
            continue
        rec = {
            "date": str(row["date"].date()),
            "close": None if math.isnan(row["close"]) else round(row["close"], 2)
        }
        records.append(rec)

    if not records:
        return {
            "publisher": publisher_code,
            "records": [],
            "msg": "No valid data after cleaning.",
            "oscSummary": {},
            "maSummary": {},
            "overallSummary": {}
        }

    # Insert oscillator/MA columns into final row
    storeIndicatorsInFinalRow(df, records, short_win, medium_win, long_win)

    # Summaries (just interpret medium signals for aggregator)
    final_idx = len(records) - 1
    last = records[final_idx]

    # 5 oscillator signals from "medium" timeframe
    oscSignals = [
        last.get("rsi_medium_sig",""),
        last.get("stoch_medium_sig",""),
        last.get("cci_medium_sig",""),
        last.get("williamsr_medium_sig",""),
        last.get("macd_medium_sig",""),
    ]
    oscSummary = build_summary(oscSignals)

    # 5 MAs from "medium"
    maSignals = [
        last.get("sma_medium_sig",""),
        last.get("ema_medium_sig",""),
        last.get("wma_medium_sig",""),
        last.get("zlema_medium_sig",""),
        last.get("boll_medium_sig",""),
    ]
    maSummary = build_summary(maSignals)

    # Overall = all 10
    overallSignals = oscSignals + maSignals
    overallSummary = build_summary(overallSignals)

    msg = f"Found {len(records)} rows (tf={tf})"
    return {
        "publisher": publisher_code,
        "records": records,
        "msg": msg,
        "oscSummary": oscSummary,
        "maSummary": maSummary,
        "overallSummary": overallSummary
    }

def build_summary(signal_list):
    """
    Takes a list of signals (e.g. ['Buy','Sell','Hold','Buy']) and returns
    a dict counting how many are 'Buy','Sell','Hold', plus a finalSignal
    that is 'Buy','Sell','Neutral'.
    """
    buy_count = signal_list.count("Buy")
    sell_count = signal_list.count("Sell")
    hold_count = signal_list.count("Hold") + signal_list.count("Neutral")
    finalSignal = compute_tv_style_signal(buy_count, sell_count)
    return {
        "buy": buy_count,
        "sell": sell_count,
        "neutral": hold_count,
        "finalSignal": finalSignal
    }

def storeIndicatorsInFinalRow(df, records, short_win, medium_win, long_win):
    """
    Adds short/medium/long oscillator & MA indicator values/signals 
    to the final record in 'records'. If the numeric portion is invalid (NaN),
    we set both the numeric value and the signal to "" so they remain consistent.
    """
    if not records or df.empty:
        return

    final_idx = len(records) - 1
    r = records[final_idx]

    # ----- Helper to finalize an indicator (val, sig) -----
    # If numeric val is None or NaN, set both to "".
    def finalize_indicator(val, sig):
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return "", ""
        # If there's a valid numeric, but no signal, default to ""
        if not sig:
            sig = ""
        return val, sig

    #
    # RSI, Stochastic, CCI, Williams, MACD calculations
    # (Unchanged logic) but we use finalize_indicator at the end.
    #

    # =========== RSI ===========
    def rsi_calc(window):
        rsi_series = RSIIndicator(df["close"], window=window, fillna=False).rsi()
        rsi_val = rsi_series.iloc[-1]
        if math.isnan(rsi_val):
            return None, None
        if rsi_val > 70:
            sig = "Sell"
        elif rsi_val < 30:
            sig = "Buy"
        else:
            sig = "Hold"
        return rsi_val, sig

    rsiS_val, rsiS_sig = rsi_calc(short_win)
    rsiS_val, rsiS_sig = finalize_indicator(rsiS_val, rsiS_sig)

    rsiM_val, rsiM_sig = rsi_calc(medium_win)
    rsiM_val, rsiM_sig = finalize_indicator(rsiM_val, rsiM_sig)

    rsiL_val, rsiL_sig = rsi_calc(long_win)
    rsiL_val, rsiL_sig = finalize_indicator(rsiL_val, rsiL_sig)

    r["rsi_short"] = rsiS_val
    r["rsi_short_sig"] = rsiS_sig
    r["rsi_medium"] = rsiM_val
    r["rsi_medium_sig"] = rsiM_sig
    r["rsi_long"] = rsiL_val
    r["rsi_long_sig"] = rsiL_sig

    # =========== Stochastic ===========
    def stoch_calc(window):
        stoch = StochasticOscillator(
            high=df["high"], low=df["low"], close=df["close"],
            window=window, smooth_window=3, fillna=False
        )
        k_val = stoch.stoch().iloc[-1]
        if math.isnan(k_val):
            return None, None
        if k_val > 80:
            sig = "Sell"
        elif k_val < 20:
            sig = "Buy"
        else:
            sig = "Hold"
        return k_val, sig

    stochS_val, stochS_sig = stoch_calc(short_win)
    stochS_val, stochS_sig = finalize_indicator(stochS_val, stochS_sig)

    stochM_val, stochM_sig = stoch_calc(medium_win)
    stochM_val, stochM_sig = finalize_indicator(stochM_val, stochM_sig)

    stochL_val, stochL_sig = stoch_calc(long_win)
    stochL_val, stochL_sig = finalize_indicator(stochL_val, stochL_sig)

    r["stoch_short"] = stochS_val
    r["stoch_short_sig"] = stochS_sig
    r["stoch_medium"] = stochM_val
    r["stoch_medium_sig"] = stochM_sig
    r["stoch_long"] = stochL_val
    r["stoch_long_sig"] = stochL_sig

    # =========== CCI ===========
    def cci_calc(window):
        cci = CCIIndicator(
            high=df["high"], low=df["low"], close=df["close"],
            window=window, fillna=False
        )
        cci_val = cci.cci().iloc[-1]
        if math.isnan(cci_val):
            return None, None
        if cci_val > 100:
            sig = "Sell"
        elif cci_val < -100:
            sig = "Buy"
        else:
            sig = "Hold"
        return cci_val, sig

    cciS_val, cciS_sig = cci_calc(short_win)
    cciS_val, cciS_sig = finalize_indicator(cciS_val, cciS_sig)

    cciM_val, cciM_sig = cci_calc(medium_win)
    cciM_val, cciM_sig = finalize_indicator(cciM_val, cciM_sig)

    cciL_val, cciL_sig = cci_calc(long_win)
    cciL_val, cciL_sig = finalize_indicator(cciL_val, cciL_sig)

    r["cci_short"] = cciS_val
    r["cci_short_sig"] = cciS_sig
    r["cci_medium"] = cciM_val
    r["cci_medium_sig"] = cciM_sig
    r["cci_long"] = cciL_val
    r["cci_long_sig"] = cciL_sig

    # =========== Williams %R ===========
    def williams_calc(lbp):
        w = WilliamsRIndicator(
            high=df["high"], low=df["low"], close=df["close"],
            lbp=lbp, fillna=False
        )
        wv = w.williams_r().iloc[-1]
        if math.isnan(wv):
            return None, None
        if wv > -20:
            sig = "Sell"
        elif wv < -80:
            sig = "Buy"
        else:
            sig = "Hold"
        return wv, sig

    wS_val, wS_sig = williams_calc(short_win)
    wS_val, wS_sig = finalize_indicator(wS_val, wS_sig)

    wM_val, wM_sig = williams_calc(medium_win)
    wM_val, wM_sig = finalize_indicator(wM_val, wM_sig)

    wL_val, wL_sig = williams_calc(long_win)
    wL_val, wL_sig = finalize_indicator(wL_val, wL_sig)

    r["williamsr_short"] = wS_val
    r["williamsr_short_sig"] = wS_sig
    r["williamsr_medium"] = wM_val
    r["williamsr_medium_sig"] = wM_sig
    r["williamsr_long"] = wL_val
    r["williamsr_long_sig"] = wL_sig

    # =========== MACD ===========
    def macd_calc(fast, slow, sign):
        macd_obj = MACD(
            close=df["close"], window_slow=slow,
            window_fast=fast, window_sign=sign, fillna=False
        )
        macd_val = macd_obj.macd().iloc[-1]
        macdsig_val = macd_obj.macd_signal().iloc[-1]
        if math.isnan(macd_val) or math.isnan(macdsig_val):
            return None, None, None
        if macd_val > macdsig_val:
            s = "Buy"
        elif macd_val < macdsig_val:
            s = "Sell"
        else:
            s = "Hold"
        return macd_val, macdsig_val, s

    macdS_val, macdS_sigVal, macdS_sig = macd_calc(6, 13, 5)
    if macdS_val is None or (isinstance(macdS_val, float) and math.isnan(macdS_val)):
        macdS_val, macdS_sigVal, macdS_sig = "", "", ""
    elif macdS_sigVal is None or (isinstance(macdS_sigVal, float) and math.isnan(macdS_sigVal)):
        macdS_sigVal = ""
    elif not macdS_sig:
        macdS_sig = ""

    macdM_val, macdM_sigVal, macdM_sig = macd_calc(12, 26, 9)
    if macdM_val is None or (isinstance(macdM_val, float) and math.isnan(macdM_val)):
        macdM_val, macdM_sigVal, macdM_sig = "", "", ""
    elif macdM_sigVal is None or (isinstance(macdM_sigVal, float) and math.isnan(macdM_sigVal)):
        macdM_sigVal = ""
    elif not macdM_sig:
        macdM_sig = ""

    macdL_val, macdL_sigVal, macdL_sig = macd_calc(24, 52, 18)
    if macdL_val is None or (isinstance(macdL_val, float) and math.isnan(macdL_val)):
        macdL_val, macdL_sigVal, macdL_sig = "", "", ""
    elif macdL_sigVal is None or (isinstance(macdL_sigVal, float) and math.isnan(macdL_sigVal)):
        macdL_sigVal = ""
    elif not macdL_sig:
        macdL_sig = ""

    r["macd_short"] = macdS_val
    r["macd_short_sig"] = macdS_sig
    r["macd_medium"] = macdM_val
    r["macd_medium_sig"] = macdM_sig
    r["macd_long"] = macdL_val
    r["macd_long_sig"] = macdL_sig

    # =========== MAs (SMA, EMA, WMA, ZLEMA, BollMid) ===========
    from ta.trend import SMAIndicator, EMAIndicator
    from ta.volatility import BollingerBands

    def compare_ma(ma_val):
        # If there's no numeric ma_val, return "".
        if ma_val is None or (isinstance(ma_val, float) and math.isnan(ma_val)):
            return ""
        close_val = df["close"].iloc[-1]
        if math.isnan(close_val) or math.isnan(ma_val):
            return ""
        if close_val > ma_val:
            return "Buy"
        elif close_val < ma_val:
            return "Sell"
        else:
            return "Hold"

    def sma_calc(window):
        sma = SMAIndicator(df["close"], window=window, fillna=False).sma_indicator().iloc[-1]
        if math.isnan(sma):
            return None
        return round(sma, 2)

    def ema_calc(window):
        ema = EMAIndicator(df["close"], window=window, fillna=False).ema_indicator().iloc[-1]
        if math.isnan(ema):
            return None
        return round(ema, 2)

    def wma_calc(window):
        if len(df) < window:
            return None
        subset = df["close"].tail(window)
        weights = range(1, window + 1)
        wma_val = sum(s * w for s, w in zip(subset, weights)) / sum(weights)
        return round(wma_val, 2)

    def zlema_calc(window):
        # Return the same as EMA or do advanced ZLEMA if you prefer
        return ema_calc(window)

    def boll_calc(window):
        boll = BollingerBands(df["close"], window=window, fillna=False)
        mid_val = boll.bollinger_mavg().iloc[-1]
        if math.isnan(mid_val):
            return None
        return round(mid_val, 2)

    # short
    smaS_val = sma_calc(short_win)
    emaS_val = ema_calc(short_win)
    wmaS_val = wma_calc(short_win)
    zlemaS_val = zlema_calc(short_win)
    bollS_val = boll_calc(short_win)

    smaS_sig = compare_ma(smaS_val)
    emaS_sig = compare_ma(emaS_val)
    wmaS_sig = compare_ma(wmaS_val)
    zlemaS_sig = compare_ma(zlemaS_val)
    bollS_sig = compare_ma(bollS_val)

    r["sma_short"] = smaS_val if smaS_val else ""
    r["sma_short_sig"] = smaS_sig
    r["ema_short"] = emaS_val if emaS_val else ""
    r["ema_short_sig"] = emaS_sig
    r["wma_short"] = wmaS_val if wmaS_val else ""
    r["wma_short_sig"] = wmaS_sig
    r["zlema_short"] = zlemaS_val if zlemaS_val else ""
    r["zlema_short_sig"] = zlemaS_sig
    r["boll_short"] = bollS_val if bollS_val else ""
    r["boll_short_sig"] = bollS_sig

    # medium
    smaM_val = sma_calc(medium_win)
    emaM_val = ema_calc(medium_win)
    wmaM_val = wma_calc(medium_win)
    zlemaM_val = zlema_calc(medium_win)
    bollM_val = boll_calc(medium_win)

    smaM_sig = compare_ma(smaM_val)
    emaM_sig = compare_ma(emaM_val)
    wmaM_sig = compare_ma(wmaM_val)
    zlemaM_sig = compare_ma(zlemaM_val)
    bollM_sig = compare_ma(bollM_val)

    r["sma_medium"] = smaM_val if smaM_val else ""
    r["sma_medium_sig"] = smaM_sig
    r["ema_medium"] = emaM_val if emaM_val else ""
    r["ema_medium_sig"] = emaM_sig
    r["wma_medium"] = wmaM_val if wmaM_val else ""
    r["wma_medium_sig"] = wmaM_sig
    r["zlema_medium"] = zlemaM_val if zlemaM_val else ""
    r["zlema_medium_sig"] = zlemaM_sig
    r["boll_medium"] = bollM_val if bollM_val else ""
    r["boll_medium_sig"] = bollM_sig

    # long
    smaL_val = sma_calc(long_win)
    emaL_val = ema_calc(long_win)
    wmaL_val = wma_calc(long_win)
    zlemaL_val = zlema_calc(long_win)
    bollL_val = boll_calc(long_win)

    smaL_sig = compare_ma(smaL_val)
    emaL_sig = compare_ma(emaL_val)
    wmaL_sig = compare_ma(wmaL_val)
    zlemaL_sig = compare_ma(zlemaL_val)
    bollL_sig = compare_ma(bollL_val)

    r["sma_long"] = smaL_val if smaL_val else ""
    r["sma_long_sig"] = smaL_sig
    r["ema_long"] = emaL_val if emaL_val else ""
    r["ema_long_sig"] = emaL_sig
    r["wma_long"] = wmaL_val if wmaL_val else ""
    r["wma_long_sig"] = wmaL_sig
    r["zlema_long"] = zlemaL_val if zlemaL_val else ""
    r["zlema_long_sig"] = zlemaL_sig
    r["boll_long"] = bollL_val if bollL_val else ""
    r["boll_long_sig"] = bollL_sig
