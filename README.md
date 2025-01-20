**Macedonian Stock Exchange Analysis Project**

## Prerequisites
- **Python 3.x** (e.g. 3.10 or 3.11)
- **Node.js & npm** (e.g. Node 18+)
- **Git** (optional, if cloning a repo)
- **Python libraries**:  
  ```bash
    &ensp; pip install flask flask-cors requests pandas ta beautifulsoup4
(For the Flask backend services.)

-**Populate the Database (Homework4 Filters)**

Navigate to the project root (e.g. cd MkStockExchageProjectHW4).

Run the filter script (e.g. filter1.py) located in Homework4/filter_service:

  &ensp; python Homework4/filter_service/filter1.py

This fetches and cleans the stock data, populating your .db files.

-**Start All Microservices (Homework4)**

Still in the root folder, go into Homework4:

 &ensp; cd Homework4

Run the batch file:

 &ensp; .\start_all.bat

(If using PowerShell, use .\start_all.bat rather than just start_all.bat.)

This launches:

The analysis_service (e.g., analysis_service_app.py)

The filter_service (e.g., filter_service_app.py)

The gateway, if applicable

Each microservice typically listens on its own port (e.g., 5000, 5001).

-**Run the Frontend (Homework2)**

Open a new terminal:

 &ensp; cd Homework2/tech_prototype/frontend

Install the Node packages:

 &ensp; npm install

This reads package.json and installs everything (React, React Router, Axios, Recharts, etc.).

If you haven’t installed chart libraries yet, run as needed:

 &ensp; npm install react-financial-charts recharts d3-time-format d3-format d3-scale

 &ensp; npm install axios react-router-dom react-datepicker

Start the React dev server:

 &ensp; npm start

The React UI is now at http://localhost:3000.

-**Usage**

Open http://localhost:3000 in your browser to access the frontend.

The frontend calls the microservices (running via the .bat script) at http://127.0.0.1:5000 (or other ports) for stock data, filtering, and analysis.
