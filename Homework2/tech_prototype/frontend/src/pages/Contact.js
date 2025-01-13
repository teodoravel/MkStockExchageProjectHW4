import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";

const Contact = () => {
  const navigate = useNavigate();
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [message, setMessage] = useState("");
  const [status, setStatus] = useState("");

  const handleSubmit = (e) => {
    e.preventDefault();
    setStatus("");

    axios
      .post("http://127.0.0.1:5000/api/users", {
        name,
        email,
        message,
      })
      .then(() => {
        setStatus("Thank you! We received your message.");
        setName("");
        setEmail("");
        setMessage("");
      })
      .catch((err) => {
        console.error("Error saving user info:", err);
        setStatus("An error occurred. Please try again later.");
      });
  };

  return (
    <div style={{ padding: "1rem" }}>
      <h2>Contact Form</h2>
      <form onSubmit={handleSubmit} style={{ maxWidth: "400px" }}>
        <div style={{ marginBottom: "0.5rem" }}>
          <label>Name:</label>
          <br />
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            style={{ width: "100%", padding: "0.5rem" }}
            required
          />
        </div>
        <div style={{ marginBottom: "0.5rem" }}>
          <label>Email:</label>
          <br />
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            style={{ width: "100%", padding: "0.5rem" }}
            required
          />
        </div>
        <div style={{ marginBottom: "0.5rem" }}>
          <label>Message:</label>
          <br />
          <textarea
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            style={{ width: "100%", padding: "0.5rem" }}
            rows="4"
            required
          />
        </div>
        <button
          type="submit"
          style={{
            backgroundColor: "#2298b5",
            color: "#fff",
            padding: "0.5rem 1rem",
            border: "none",
            borderRadius: "4px",
            cursor: "pointer",
          }}
        >
          Submit
        </button>
      </form>
      {status && <p style={{ marginTop: "1rem", color: "green" }}>{status}</p>}
      <button
        style={{
          marginTop: "1rem",
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

export default Contact;
