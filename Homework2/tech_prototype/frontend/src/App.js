import React from "react";
import { BrowserRouter as Router, Routes, Route, Link } from "react-router-dom";
import Home from "./pages/Home";
import ChartsInfo from "./pages/ChartsInfo";
import Contact from "./pages/Contact";
// REPLACE old import with new
import TechnicalAnalysis from "./pages/TechnicalAnalysis";

function App() {
  const appStyle = {
    minHeight: "100vh",
    background: "linear-gradient(to bottom right, #f0f4f8, #c2e0f4)",
    color: "#333",
    fontFamily: "Arial, sans-serif",
    padding: "1rem",
  };

  const navStyle = {
    marginBottom: "1rem",
    background: "#89c2d9",
    padding: "0.5rem 1rem",
    borderRadius: "8px",
  };

  const linkStyle = {
    marginRight: "1rem",
    color: "#fff",
    textDecoration: "none",
    fontWeight: "bold",
  };

  return (
    <div style={appStyle}>
      <Router>
        <nav style={navStyle}>
          <Link to="/" style={linkStyle}>
            Home
          </Link>
          <Link to="/chartsinfo" style={linkStyle}>
            Charts & Info
          </Link>
          <Link to="/contact" style={linkStyle}>
            Contact
          </Link>
          <Link to="/technical-analysis" style={linkStyle}>
            Technical Analysis
          </Link>
        </nav>

        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/chartsinfo" element={<ChartsInfo />} />
          <Route path="/contact" element={<Contact />} />
          <Route path="/technical-analysis" element={<TechnicalAnalysis />} />
        </Routes>
      </Router>
    </div>
  );
}

export default App;
