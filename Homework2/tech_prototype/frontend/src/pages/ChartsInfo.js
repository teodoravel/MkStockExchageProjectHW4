import React, { useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import axios from "axios";
import ReactDatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

/**
 * Parse "DD.MM.YYYY" -> numeric timestamp
 * e.g., "06.10.2015" => new Date(2015, 9, 6).getTime()
 */
function parseTimestamp(dateStr) {
  const parts = dateStr.split(".");
  if (parts.length === 3) {
    const day = parseInt(parts[0], 10);
    const month = parseInt(parts[1], 10) - 1; // zero-based
    const year = parseInt(parts[2], 10);
    if (!isNaN(day) && !isNaN(month) && !isNaN(year)) {
      return new Date(year, month, day).getTime();
    }
  }
  return null;
}

/**
 * Build yearly ticks for the X-axis, from minTime to maxTime,
 * labeling just one tick per year.
 */
function buildYearTicks(minTime, maxTime) {
  const minYear = new Date(minTime).getFullYear();
  const maxYear = new Date(maxTime).getFullYear();
  const ticks = [];
  for (let y = minYear; y <= maxYear; y++) {
    ticks.push(new Date(y, 0, 1).getTime());
  }
  return ticks;
}

/**
 * Custom Tooltip to display original fullDate in hover popups.
 */
function CustomTooltip({ active, payload }) {
  if (active && payload && payload.length) {
    const record = payload[0].payload;
    return (
      <div
        style={{
          background: "#fff",
          border: "1px solid #ccc",
          padding: "0.5rem",
          borderRadius: "4px",
        }}
      >
        <p>Date: {record.fullDate}</p>
        {payload.map((item, i) => (
          <p key={i} style={{ color: item.color }}>
            {item.name}: {item.value}
          </p>
        ))}
      </div>
    );
  }
  return null;
}

const ChartsInfo = () => {
  const navigate = useNavigate();
  const location = useLocation();

  const [publisher, setPublisher] = useState(location.state?.publisher || "");
  const [records, setRecords] = useState([]);

  // Optional date range
  const [startDate, setStartDate] = useState(null);
  const [endDate, setEndDate] = useState(null);

  // Summary for the entire dataset
  const [summary, setSummary] = useState({
    highestPrice: 0,
    lowestPrice: 0,
    overallChange: "0%",
    averagePrice: 0,
    totalVolume: 0,
    totalTurnover: 0,
  });

  useEffect(() => {
    if (!publisher) return;
    axios
      .get(`http://127.0.0.1:5000/api/stock_data?publisher=${publisher}`)
      .then((res) => {
        // Suppose backend returns { "publisher": "...", "records": [...] }
        if (res.data.records && res.data.records.length) {
          const enriched = res.data.records.map((r) => {
            const timestamp = parseTimestamp(r.date);
            const priceVal = parseFloat((r.price || "0").replace(",", "")) || 0;
            const volumeVal =
              parseFloat((r.volume || "0").replace(",", "")) || 0;
            return {
              ...r,
              fullDate: r.date,
              timestamp,
              priceVal,
              volumeVal,
            };
          });

          // Filter out invalid timestamps and sort ascending
          const validData = enriched.filter((x) => x.timestamp !== null);
          validData.sort((a, b) => a.timestamp - b.timestamp);

          setRecords(validData);
          computeSummary(validData);
        } else {
          setRecords([]);
          resetSummary();
        }
      })
      .catch((err) => console.error("Error fetching stock data:", err));
  }, [publisher]);

  // Summary of the entire dataset
  const computeSummary = (data) => {
    if (!data.length) {
      resetSummary();
      return;
    }
    const prices = data.map((d) => d.priceVal);
    const volumes = data.map((d) => d.volumeVal);
    const turnovers = data.map(
      (d) => parseFloat((d.total_turnover || "0").replace(",", "")) || 0
    );

    const highestPrice = Math.max(...prices);
    const lowestPrice = Math.min(...prices);
    const averagePrice = (
      prices.reduce((a, b) => a + b, 0) / prices.length
    ).toFixed(2);

    const totalVolume = volumes.reduce((a, b) => a + b, 0);
    const totalTurnover = turnovers.reduce((a, b) => a + b, 0);

    // Compare first vs last in time
    const firstPrice = prices[0];
    const lastPrice = prices[prices.length - 1];
    const pctChange = firstPrice
      ? (((lastPrice - firstPrice) / firstPrice) * 100).toFixed(2) + "%"
      : "0%";

    setSummary({
      highestPrice,
      lowestPrice,
      overallChange: pctChange,
      averagePrice,
      totalVolume,
      totalTurnover,
    });
  };

  const resetSummary = () => {
    setSummary({
      highestPrice: 0,
      lowestPrice: 0,
      overallChange: "0%",
      averagePrice: 0,
      totalVolume: 0,
      totalTurnover: 0,
    });
  };

  /**
   * Filter records to the chosen date range (or all if none chosen).
   */
  const filteredRecords = useMemo(() => {
    if (!records.length) return [];

    if (!startDate && !endDate) {
      return records; // entire dataset
    }

    const startTS = startDate ? startDate.getTime() : Number.MIN_SAFE_INTEGER;
    const endTS = endDate ? endDate.getTime() : Number.MAX_SAFE_INTEGER;

    return records.filter(
      (rec) => rec.timestamp >= startTS && rec.timestamp <= endTS
    );
  }, [records, startDate, endDate]);

  // Build domain & ticks for the filtered subset
  let minTime = 0;
  let maxTime = 0;
  if (filteredRecords.length) {
    minTime = filteredRecords[0].timestamp;
    maxTime = filteredRecords[filteredRecords.length - 1].timestamp;
  }
  const yearTicks =
    filteredRecords.length > 0 ? buildYearTicks(minTime, maxTime) : [];

  // For the title, e.g. (01.01.2014 - Present)
  const rangeLabel = () => {
    if (!startDate && !endDate) return "(Full Range)";
    const startStr = startDate
      ? startDate.toLocaleDateString("en-GB")
      : "Beginning";
    const endStr = endDate ? endDate.toLocaleDateString("en-GB") : "Present";
    return `(${startStr} - ${endStr})`;
  };

  // Export only the filtered dataset
  const handleExportCSV = () => {
    if (!filteredRecords.length) return;
    const headers = "timestamp,price,volume,fullDate";
    const rows = filteredRecords.map(
      (r) => `${r.timestamp},${r.priceVal},${r.volumeVal},${r.fullDate}`
    );
    const csvContent = [headers, ...rows].join("\n");
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);

    const link = document.createElement("a");
    link.href = url;
    link.setAttribute("download", `StockData_${publisher}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return (
    <div style={{ padding: "1rem" }}>
      <h2>
        {publisher || "No Publisher Selected"} - Charts &amp; Info{" "}
        <span style={{ fontSize: "0.8em" }}>{rangeLabel()}</span>
      </h2>

      {/* Date range pickers */}
      <div style={{ marginBottom: "1rem" }}>
        <label style={{ marginRight: "0.5rem" }}>Start Date:</label>
        <ReactDatePicker
          selected={startDate}
          onChange={(date) => setStartDate(date)}
          dateFormat="dd.MM.yyyy"
          isClearable
          placeholderText="No start date"
        />
        <label style={{ margin: "0 0.5rem" }}>End Date:</label>
        <ReactDatePicker
          selected={endDate}
          onChange={(date) => setEndDate(date)}
          dateFormat="dd.MM.yyyy"
          isClearable
          placeholderText="No end date"
        />
      </div>

      {/* Volume Chart */}
      <div
        style={{
          marginBottom: "1rem",
          background: "#f9f9ff",
          borderRadius: "8px",
          padding: "1rem",
        }}
      >
        <h3>Trading Volume</h3>
        {!filteredRecords.length ? (
          <p>No data available.</p>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={filteredRecords}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                type="number"
                dataKey="timestamp"
                domain={[minTime, maxTime]}
                ticks={yearTicks}
                allowDecimals={false}
                tickFormatter={(val) => new Date(val).getFullYear()}
              />
              <YAxis />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Line
                type="monotone"
                dataKey="volumeVal"
                stroke="#8884d8"
                strokeWidth={2}
                dot={{ r: 3 }}
                name="Volume"
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Price Chart */}
      <div
        style={{
          marginBottom: "1rem",
          background: "#f9f9ff",
          borderRadius: "8px",
          padding: "1rem",
        }}
      >
        <h3>Stock Price</h3>
        {!filteredRecords.length ? (
          <p>No data available.</p>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={filteredRecords}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                type="number"
                dataKey="timestamp"
                domain={[minTime, maxTime]}
                ticks={yearTicks}
                allowDecimals={false}
                tickFormatter={(val) => new Date(val).getFullYear()}
              />
              <YAxis />
              <Tooltip content={<CustomTooltip />} />
              <Legend />
              <Line
                type="monotone"
                dataKey="priceVal"
                stroke="#82ca9d"
                strokeWidth={2}
                dot={{ r: 3 }}
                name="Price"
              />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Summary for entire dataset */}
      <div
        style={{
          marginBottom: "1rem",
          background: "#e0f3ff",
          borderRadius: "8px",
          padding: "1rem",
        }}
      >
        <h3>Summary Metrics (All Data)</h3>
        <p>Highest Price: {summary.highestPrice}</p>
        <p>Lowest Price: {summary.lowestPrice}</p>
        <p>Overall Change: {summary.overallChange}</p>
        <p>Average Price: {summary.averagePrice}</p>
        <p>Total Volume: {summary.totalVolume}</p>
        <p>Total Turnover: {summary.totalTurnover}</p>
      </div>

      <button
        style={{
          backgroundColor: "#2298b5",
          color: "#fff",
          padding: "0.5rem 1rem",
          border: "none",
          borderRadius: "4px",
          cursor: "pointer",
          marginRight: "1rem",
        }}
        onClick={handleExportCSV}
      >
        Export to CSV
      </button>

      <button
        style={{
          backgroundColor: "#555",
          color: "#fff",
          padding: "0.5rem 1rem",
          border: "none",
          borderRadius: "4px",
          cursor: "pointer",
        }}
        onClick={() => navigate("/")}
      >
        Back to Home
      </button>
    </div>
  );
};

export default ChartsInfo;
