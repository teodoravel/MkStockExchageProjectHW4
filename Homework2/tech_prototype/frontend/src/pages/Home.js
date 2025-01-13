import React, { useEffect, useState } from "react";
import axios from "axios";
import { useNavigate } from "react-router-dom";

const Home = () => {
  const navigate = useNavigate();
  const [publishers, setPublishers] = useState([]);
  const [selectedPublisher, setSelectedPublisher] = useState("");

  useEffect(() => {
    axios
      .get("http://127.0.0.1:5000/api/publishers")
      .then((res) => {
        if (res.data.publishers) {
          // Sort alphabetically
          const sorted = [...res.data.publishers].sort();
          setPublishers(sorted);
          if (sorted.length > 0) {
            setSelectedPublisher(sorted[0]);
          }
        }
      })
      .catch((err) => {
        console.error("Error fetching publishers:", err);
      });
  }, []);

  const handlePublisherChange = (e) => {
    setSelectedPublisher(e.target.value);
  };

  const goToChartsInfo = () => {
    if (!selectedPublisher) return;
    navigate("/chartsinfo", { state: { publisher: selectedPublisher } });
  };

  // NEW: Navigate to Technical Analysis
  const goToTechnicalAnalysis = () => {
    if (!selectedPublisher) return;
    navigate("/technical-analysis", {
      state: { publisher: selectedPublisher },
    });
  };

  return (
    <div style={{ padding: "1rem" }}>
      <h1>Macedonian Stock Exchange Analysis</h1>
      <h2>Home Screen</h2>
      <p>Select a company to analyze:</p>
      <select value={selectedPublisher} onChange={handlePublisherChange}>
        {publishers.map((pub) => (
          <option key={pub} value={pub}>
            {pub}
          </option>
        ))}
      </select>

      <div style={{ marginTop: "1rem" }}>
        <button
          style={{
            backgroundColor: "#2298b5",
            color: "#fff",
            padding: "0.5rem 1rem",
            border: "none",
            borderRadius: "4px",
            cursor: "pointer",
            marginRight: "0.5rem",
          }}
          onClick={goToChartsInfo}
        >
          Go to Charts & Info
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
          onClick={goToTechnicalAnalysis}
        >
          Technical Analysis
        </button>
      </div>
    </div>
  );
};

export default Home;
