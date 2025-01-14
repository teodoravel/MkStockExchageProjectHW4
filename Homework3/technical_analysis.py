import sqlite3
import pandas as pd
import math
from pathlib import Path

# TA library
import ta
from ta.momentum import StochasticOscillator, RSIIndicator, WilliamsRIndicator
from ta.trend import CCIIndicator, MACD, SMAIndicator, EMAIndicator
from ta.volatility import BollingerBands

# Points to the local stock_data.db in the same folder as this file
STOCK_DB_PATH = Path(__file__).parent / "stock_data.db"

def parse_euro_number(val_str):
    """Convert '2.140,00' -> '2140,00' -> '2140.00' -> float."""
    if val_str in ("", "None", "nan"):
        return None
    step1 = val_str.replace(".", "")  # remove thousands
    step2 = step1.replace(",", ".")
    return step2

def compute_tv_style_signal(buy_count, sell_count):
    """If buys > sells => 'Buy', else 'Sell' or 'Neutral'."""
    if buy_count > sell_count:
        return "Buy"
    elif sell_count > buy_count:
        return "Sell"
    else:
        return "Neutral"

def compute_all_indicators_and_aggregate(publisher_code, tf="1D"):
    """
    Extended to have 5 MAs total: SMA, EMA, WMA, ZLEMA, BollMid,
    each done short/medium/long, plus your 5 original oscillators (RSI,Stoch,CCI,WR,MACD).

    We'll store them in the final row of 'records' with keys like:
      wma_short, wma_short_sig, wma_medium, wma_medium_sig, ...
    Then the aggregator counts them for maSummary + overallSummary.
    """
    # 1) Query the DB
    conn = sqlite3.connect(STOCK_DB_PATH)
    query = """
        SELECT date, price, quantity, max, min
        FROM stock_data
        WHERE publisher_code = ?
        ORDER BY date ASC
    """
    df = pd.read_sql_query(query, conn, params=[publisher_code])
    conn.close()

    # 2) If no data found, return blank
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

    # Convert date + sort
    df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")
    df.sort_values("date", inplace=True, ignore_index=True)

    # Convert numeric columns from '2.140,00' style
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

    # short=7, medium=14, long=30
    short_win = 7
    medium_win = 14
    long_win = 30

    # Build a simple list of daily records
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

    # Insert oscillator/MA columns into the final row
    storeIndicatorsInFinalRow(df, records, short_win, medium_win, long_win)

    # Summaries (just interpret medium signals for aggregator)
    final_idx = len(records) - 1
    if final_idx < 0:
        return {
            "publisher": publisher_code,
            "records": records,
            "msg": f"Found {len(records)} rows (tf={tf}) but no final row?",
            "oscSummary": {},
            "maSummary": {},
            "overallSummary": {}
        }

    last = records[final_idx]

    # Collect 5 oscillator signals from the "medium" timeframe
    oscSignals = [
        last.get("rsi_medium_sig",""),
        last.get("stoch_medium_sig",""),
        last.get("cci_medium_sig",""),
        last.get("williamsr_medium_sig",""),
        last.get("macd_medium_sig",""),
    ]
    oscSummary = build_summary(oscSignals)

    # Collect 5 MAs from "medium"
    maSignals = [
        last.get("sma_medium_sig",""),
        last.get("ema_medium_sig",""),
        last.get("wma_medium_sig",""),
        last.get("zlema_medium_sig",""),
        last.get("boll_medium_sig",""),
    ]
    maSummary = build_summary(maSignals)

    # overall = all 10
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
    """Count 'Buy', 'Sell', 'Hold/Neutral' signals and produce a finalSignal."""
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
    Full short/medium/long logic for the 5 oscillators + 5 MAs:
      - RSI, Stoch, CCI, WilliamsR, MACD
      - SMA, EMA, WMA, ZLEMA, BollMid
    Then store them in records[-1].
    """
    if not records or df.empty:
        return

    final_idx = len(records) - 1
    r = records[final_idx]

    # ---------- OSCILLATOR HELPER FUNCS ----------
    from ta.momentum import RSIIndicator, StochasticOscillator, WilliamsRIndicator
    from ta.trend import CCIIndicator, MACD

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
        return round(rsi_val, 2), sig

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
        return round(k_val, 2), sig

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
        return round(cci_val, 2), sig

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
        return round(wv, 2), sig

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
        return round(macd_val, 2), round(macdsig_val, 2), s

    # RSI short, medium, long
    rsiS_val, rsiS_sig = rsi_calc(short_win)
    rsiM_val, rsiM_sig = rsi_calc(medium_win)
    rsiL_val, rsiL_sig = rsi_calc(long_win)

    # Stoch
    stochS_val, stochS_sig = stoch_calc(short_win)
    stochM_val, stochM_sig = stoch_calc(medium_win)
    stochL_val, stochL_sig = stoch_calc(long_win)

    # CCI
    cciS_val, cciS_sig = cci_calc(short_win)
    cciM_val, cciM_sig = cci_calc(medium_win)
    cciL_val, cciL_sig = cci_calc(long_win)

    # Williams %R
    wS_val, wS_sig = williams_calc(short_win)
    wM_val, wM_sig = williams_calc(medium_win)
    wL_val, wL_sig = williams_calc(long_win)

    # MACD short=(6,13,5), medium=(12,26,9), long=(24,52,18)
    macdS_val, macdS_sigVal, macdS_sig = macd_calc(6, 13, 5)
    macdM_val, macdM_sigVal, macdM_sig = macd_calc(12, 26, 9)
    macdL_val, macdL_sigVal, macdL_sig = macd_calc(24, 52, 18)

    # ---------- MOVING AVERAGE HELPER FUNCS ----------
    from ta.trend import SMAIndicator, EMAIndicator
    from ta.volatility import BollingerBands

    def compare_ma(ma_val):
        if ma_val is None:
            return None
        close_val = df["close"].iloc[-1]
        if math.isnan(close_val) or math.isnan(ma_val):
            return None
        if close_val > ma_val:
            return "Buy"
        elif close_val < ma_val:
            return "Sell"
        else:
            return "Hold"

    def sma_calc(window):
        sma = SMAIndicator(df["close"], window=window, fillna=False).sma_indicator().iloc[-1]
        return None if math.isnan(sma) else round(sma, 2)

    def ema_calc(window):
        ema = EMAIndicator(df["close"], window=window, fillna=False).ema_indicator().iloc[-1]
        return None if math.isnan(ema) else round(ema, 2)

    def wma_calc(window):
        # Simplistic Weighted MA approach
        if len(df) < window:
            return None
        subset = df["close"].tail(window)
        weights = range(1, window + 1)
        wma_val = sum(s * w for s, w in zip(subset, weights)) / sum(weights)
        return round(wma_val, 2)

    def zlema_calc(window):
        # For demonstration, just returning EMA
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

    # Insert them into r
    # RSI
    r["rsi_short"] = rsiS_val or ""
    r["rsi_short_sig"] = rsiS_sig or ""
    r["rsi_medium"] = rsiM_val or ""
    r["rsi_medium_sig"] = rsiM_sig or ""
    r["rsi_long"] = rsiL_val or ""
    r["rsi_long_sig"] = rsiL_sig or ""

    # Stoch
    r["stoch_short"] = stochS_val or ""
    r["stoch_short_sig"] = stochS_sig or ""
    r["stoch_medium"] = stochM_val or ""
    r["stoch_medium_sig"] = stochM_sig or ""
    r["stoch_long"] = stochL_val or ""
    r["stoch_long_sig"] = stochL_sig or ""

    # CCI
    r["cci_short"] = cciS_val or ""
    r["cci_short_sig"] = cciS_sig or ""
    r["cci_medium"] = cciM_val or ""
    r["cci_medium_sig"] = cciM_sig or ""
    r["cci_long"] = cciL_val or ""
    r["cci_long_sig"] = cciL_sig or ""

    # Williams %R
    r["williamsr_short"] = wS_val or ""
    r["williamsr_short_sig"] = wS_sig or ""
    r["williamsr_medium"] = wM_val or ""
    r["williamsr_medium_sig"] = wM_sig or ""
    r["williamsr_long"] = wL_val or ""
    r["williamsr_long_sig"] = wL_sig or ""

    # MACD
    r["macd_short"] = macdS_val or ""
    r["macd_short_sig"] = macdS_sig or ""
    r["macd_medium"] = macdM_val or ""
    r["macd_medium_sig"] = macdM_sig or ""
    r["macd_long"] = macdL_val or ""
    r["macd_long_sig"] = macdL_sig or ""

    # SMA
    r["sma_short"] = smaS_val or ""
    r["sma_short_sig"] = smaS_sig or ""
    r["sma_medium"] = smaM_val or ""
    r["sma_medium_sig"] = smaM_sig or ""
    r["sma_long"] = smaL_val or ""
    r["sma_long_sig"] = smaL_sig or ""

    # EMA
    r["ema_short"] = emaS_val or ""
    r["ema_short_sig"] = emaS_sig or ""
    r["ema_medium"] = emaM_val or ""
    r["ema_medium_sig"] = emaM_sig or ""
    r["ema_long"] = emaL_val or ""
    r["ema_long_sig"] = emaL_sig or ""

    # WMA
    r["wma_short"] = wmaS_val or ""
    r["wma_short_sig"] = wmaS_sig or ""
    r["wma_medium"] = wmaM_val or ""
    r["wma_medium_sig"] = wmaM_sig or ""
    r["wma_long"] = wmaL_val or ""
    r["wma_long_sig"] = wmaL_sig or ""

    # ZLEMA
    r["zlema_short"] = zlemaS_val or ""
    r["zlema_short_sig"] = zlemaS_sig or ""
    r["zlema_medium"] = zlemaM_val or ""
    r["zlema_medium_sig"] = zlemaM_sig or ""
    r["zlema_long"] = zlemaL_val or ""
    r["zlema_long_sig"] = zlemaL_sig or ""

    # BollMid
    r["boll_short"] = bollS_val or ""
    r["boll_short_sig"] = bollS_sig or ""
    r["boll_medium"] = bollM_val or ""
    r["boll_medium_sig"] = bollM_sig or ""
    r["boll_long"] = bollL_val or ""
    r["boll_long_sig"] = bollL_sig or ""
