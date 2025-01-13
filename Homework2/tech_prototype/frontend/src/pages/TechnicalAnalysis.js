import React, { useEffect, useState } from "react";
import { useLocation } from "react-router-dom";
import axios from "axios";

// from react-financial-charts
import {
  ChartCanvas,
  Chart,
  CandlestickSeries,
  discontinuousTimeScaleProvider,
  XAxis,
  YAxis,
  MouseCoordinateX,
  MouseCoordinateY,
} from "react-financial-charts";

// d3
import { timeFormat } from "d3-time-format";
import { format } from "d3-format";

function TechnicalAnalysis() {
  const location = useLocation();
  const [publisher, setPublisher] = useState(location.state?.publisher || "");
  // timeframe = "1D","1W","1M"
  const [timeframe, setTimeframe] = useState("1D");

  const [candles, setCandles] = useState([]);
  const [records, setRecords] = useState([]);
  const [message, setMessage] = useState("");

  // Decide which frames to show based on timeframe:
  // "1D" => "short"
  // "1W" => "medium"
  // "1M" => "long"
  const [framesToShow, setFramesToShow] = useState(["short"]);

  useEffect(() => {
    if (timeframe === "1D") {
      setFramesToShow(["short"]);
    } else if (timeframe === "1W") {
      setFramesToShow(["medium"]);
    } else if (timeframe === "1M") {
      setFramesToShow(["long"]);
    }
  }, [timeframe]);

  useEffect(() => {
    if (!publisher) return;

    const url = `http://127.0.0.1:5000/api/technical_analysis?publisher=${publisher}&tf=${timeframe}`;
    axios
      .get(url)
      .then((res) => {
        if (res.data && Array.isArray(res.data.records)) {
          setMessage(res.data.msg || "");

          // Convert to candlestick
          const candleData = buildCandleData(res.data.records);
          setCandles(candleData);

          // Save all short/medium/long fields in records
          setRecords(res.data.records);
        } else {
          setMessage("No records found or invalid response.");
          setCandles([]);
          setRecords([]);
        }
      })
      .catch((err) => {
        console.error("Error fetching data:", err);
        setMessage("Error fetching data.");
        setCandles([]);
        setRecords([]);
      });
  }, [publisher, timeframe]);

  function handleTimeframeClick(tf) {
    setTimeframe(tf);
  }

  function getBtnStyle(tf) {
    return {
      backgroundColor: timeframe === tf ? "#2298b5" : "#ccc",
      color: "#fff",
      padding: "0.4rem 0.8rem",
      border: "none",
      borderRadius: "4px",
      marginRight: "0.5rem",
      cursor: "pointer",
    };
  }

  // Summaries for short/medium/long
  const summaryData = buildLocalSummary(records, framesToShow);

  return (
    <div style={{ padding: "1rem" }}>
      <h2>Technical Analysis for {publisher}</h2>
      <p>{message}</p>

      {/* Timeframe Buttons */}
      <div style={{ marginBottom: "1rem" }}>
        <button
          style={getBtnStyle("1D")}
          onClick={() => handleTimeframeClick("1D")}
        >
          1 Day
        </button>
        <button
          style={getBtnStyle("1W")}
          onClick={() => handleTimeframeClick("1W")}
        >
          1 Week
        </button>
        <button
          style={getBtnStyle("1M")}
          onClick={() => handleTimeframeClick("1M")}
        >
          1 Month
        </button>
      </div>

      {/* Candlestick Chart */}
      {candles.length > 0 ? (
        <div style={{ marginBottom: "2rem" }}>
          <CandlestickChart data={candles} width={800} height={400} />
        </div>
      ) : (
        <p>No candlestick data.</p>
      )}

      {/* Summary Boxes */}
      <div style={{ display: "flex", justifyContent: "space-around" }}>
        <SummaryBox title="Oscillators" data={summaryData.oscSummary} />
        <SummaryBox title="Moving Averages" data={summaryData.maSummary} />
        <SummaryBox title="Overall" data={summaryData.overallSummary} />
      </div>

      {/* Single Table, but only shows the chosen frame(s) */}
      <div style={{ marginTop: "2rem", overflowX: "auto" }}>
        <IndicatorsTable records={records} framesToShow={framesToShow} />
      </div>
    </div>
  );
}

/** Re-aggregate signals locally for the chosen frames */
function buildLocalSummary(records, framesToShow) {
  if (!records || records.length === 0) {
    return {
      oscSummary: { buy: 0, sell: 0, neutral: 0, finalSignal: "N/A" },
      maSummary: { buy: 0, sell: 0, neutral: 0, finalSignal: "N/A" },
      overallSummary: { buy: 0, sell: 0, neutral: 0, finalSignal: "N/A" },
    };
  }

  const lastRow = records[records.length - 1];

  // 5 oscillators
  const oscillators = ["rsi_", "stoch_", "cci_", "williamsr_", "macd_"];
  // 5 MAs
  const movingAvgs = ["sma_", "ema_", "wma_", "zlema_", "boll_"];

  const oscSignals = [];
  oscillators.forEach((prefix) => {
    framesToShow.forEach((tf) => {
      const sigKey = prefix + tf + "_sig";
      const sigVal = lastRow[sigKey];
      if (sigVal) {
        oscSignals.push(sigVal);
      }
    });
  });

  const maSignals = [];
  movingAvgs.forEach((prefix) => {
    framesToShow.forEach((tf) => {
      const sigKey = prefix + tf + "_sig";
      const sigVal = lastRow[sigKey];
      if (sigVal) {
        maSignals.push(sigVal);
      }
    });
  });

  const oscSummary = buildSummary(oscSignals);
  const maSummary = buildSummary(maSignals);
  const overallSignals = [...oscSignals, ...maSignals];
  const overallSummary = buildSummary(overallSignals);

  return { oscSummary, maSummary, overallSummary };
}

/** aggregator for buy/sell/hold signals */
function buildSummary(signalArray) {
  let buy = 0,
    sell = 0,
    hold = 0;
  signalArray.forEach((sig) => {
    if (sig === "Buy") buy++;
    else if (sig === "Sell") sell++;
    else hold++;
  });
  let finalSignal = "Neutral";
  if (buy > sell) finalSignal = "Buy";
  else if (sell > buy) finalSignal = "Sell";
  return { buy, sell, neutral: hold, finalSignal };
}

/**
 * Single table, but only shows the chosen frames.
 * Key style changes for auto sizing are shown below.
 */
function IndicatorsTable({ records, framesToShow }) {
  if (!records || records.length === 0) {
    return <p>No indicator data available.</p>;
  }
  const lastRow = records[records.length - 1];

  // 5 oscillators, 5 MAs
  const oscillators = ["rsi_", "stoch_", "cci_", "williamsr_", "macd_"];
  const movingAvgs = ["sma_", "ema_", "wma_", "zlema_", "boll_"];
  const allIndicators = [...oscillators, ...movingAvgs];

  return (
    <table
      style={{
        tableLayout: "auto", // auto-size columns
        borderCollapse: "collapse",
        whiteSpace: "nowrap", // keep each cell on one line
      }}
    >
      <thead>
        <tr style={{ background: "#eee" }}>
          <th style={cellStyle}>Indicator</th>
          {framesToShow.map((tf) => (
            <React.Fragment key={tf}>
              <th style={cellStyle}>{tf} Value</th>
              <th style={cellStyle}>{tf} Signal</th>
            </React.Fragment>
          ))}
        </tr>
      </thead>
      <tbody>
        {allIndicators.map((prefix, index) => {
          const rowCells = [];
          rowCells.push(
            <td style={cellStyle} key={`name-${index}`}>
              {getIndicatorName(prefix)}
            </td>
          );

          framesToShow.forEach((tf) => {
            const valKey = prefix + tf;
            const sigKey = prefix + tf + "_sig";
            const val = lastRow[valKey] ?? "";
            const sig = lastRow[sigKey] ?? "";
            rowCells.push(
              <td style={cellStyle} key={`${valKey}-${index}`}>
                {val}
              </td>
            );
            rowCells.push(
              <td style={cellStyle} key={`${sigKey}-${index}`}>
                {sig}
              </td>
            );
          });
          return <tr key={`row-${index}`}>{rowCells}</tr>;
        })}
      </tbody>
    </table>
  );
}

function getIndicatorName(prefix) {
  if (prefix.startsWith("rsi_")) return "RSI (Oscillator)";
  if (prefix.startsWith("stoch_")) return "Stoch (Oscillator)";
  if (prefix.startsWith("cci_")) return "CCI (Oscillator)";
  if (prefix.startsWith("williamsr_")) return "Williams %R (Oscillator)";
  if (prefix.startsWith("macd_")) return "MACD (Oscillator)";
  if (prefix.startsWith("sma_")) return "SMA (Moving Average)";
  if (prefix.startsWith("ema_")) return "EMA (Moving Average)";
  if (prefix.startsWith("wma_")) return "WMA (Moving Average)";
  if (prefix.startsWith("zlema_")) return "ZLEMA (Moving Average)";
  if (prefix.startsWith("boll_")) return "BollMid (Moving Average)";
  return prefix;
}

function buildCandleData(records) {
  if (!records || records.length === 0) return [];
  let prevClose = records[0].close || 0;
  return records.map((r, idx) => {
    const openVal = idx === 0 ? r.close : prevClose;
    const highVal = r.high ?? Math.max(openVal, r.close);
    const lowVal = r.low ?? Math.min(openVal, r.close);
    prevClose = r.close;

    return {
      date: new Date(r.date),
      open: openVal,
      high: highVal,
      low: lowVal,
      close: r.close,
      volume: 0,
    };
  });
}

/** chart subcomponent */
function CandlestickChart({ data, width, height }) {
  const xScaleProvider = discontinuousTimeScaleProvider.inputDateAccessor(
    (d) => d.date
  );
  const {
    data: chartData,
    xScale,
    xAccessor,
    displayXAccessor,
  } = xScaleProvider(data);
  const max = xAccessor(chartData[chartData.length - 1]);
  const min = xAccessor(chartData[0]);
  const xExtents = [min, max];
  const margin = { left: 50, right: 50, top: 10, bottom: 30 };

  const xDisplayFormat = timeFormat("%Y-%m-%d");
  const yDisplayFormat = format(".2f");

  return (
    <ChartCanvas
      height={height}
      width={width}
      ratio={1}
      margin={margin}
      data={chartData}
      xScale={xScale}
      xAccessor={xAccessor}
      displayXAccessor={displayXAccessor}
      xExtents={xExtents}
    >
      <Chart id={1} yExtents={(d) => [d.high, d.low]}>
        <XAxis />
        <YAxis />
        <MouseCoordinateX displayFormat={xDisplayFormat} />
        <MouseCoordinateY displayFormat={yDisplayFormat} />
        <CandlestickSeries />
      </Chart>
    </ChartCanvas>
  );
}

/** small summary box */
function SummaryBox({ title, data }) {
  return (
    <div
      style={{
        border: "1px solid #ccc",
        borderRadius: "8px",
        padding: "1rem",
        width: "180px",
        textAlign: "center",
        margin: "1rem",
      }}
    >
      <h4>{title}</h4>
      <p>Buy: {data.buy || 0}</p>
      <p>Sell: {data.sell || 0}</p>
      <p>Neutral: {data.neutral || 0}</p>
      <p style={{ fontWeight: "bold" }}>Final: {data.finalSignal || "N/A"}</p>
    </div>
  );
}

const cellStyle = {
  border: "1px solid #ccc",
  padding: "0.5rem",
};

export default TechnicalAnalysis;
