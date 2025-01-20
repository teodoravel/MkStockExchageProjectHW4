Macedonian Stock Exchange Analysis Project

## Prerequisites
- Python 3.x (e.g., 3.10 or 3.11)
- Node.js & npm (e.g., Node 18+)
- Git (optional)
- Python libraries: `pip install flask flask-cors requests pandas ta beautifulsoup4` (for the Flask backend services)

---

1. Populate the Database (Homework4 Filters)
 Navigate to the project root (e.g. `cd MkStockExchageProjectHW4`).
 Run the filter script (e.g., `filter1.py`) located in **Homework4/filter_service**:
   ```bash
   python Homework4/filter_service/filter1.py

2. Start All Microservices (Homework4)
Still in the root folder (Homework4/), run the batch file:
cd Homework4
.\start_all.bat
(If using PowerShell, use .\start_all.bat instead of just start_all.bat.)

 This launches:
 The analysis_service (e.g., analysis_service_app.py)
The filter_service (e.g., filter_service_app.py)
The gateway or other scripts if applicable
Each microservice typically listens on a different port (5000, 5001, etc.).

3. Run the Frontend (Homework2)
In a new terminal, navigate to Homework2/tech_prototype/frontend:
cd Homework2/tech_prototype/frontend
Install the Node packages:
npm install
Start the React dev server:
npm start
Your React UI is now at http://localhost:3000.

4. Usage
Open http://localhost:3000 in your browser to access the frontend.
The frontend calls the gateway/microservices running via the .bat script (e.g. http://127.0.0.1:5000) for stock data, filtering, and analysis.

