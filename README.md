**HOMEWORK 3 CODES: The codes for the third homework (technical analysis) are located in Homework2\tech_prototype and Homework3. The frontend is in Homework2\tech_prototype\frontend.


**Macedonian Stock Exchange Analysis Project Structure:**

Homework 1
Location: Homework1
Filters: filter1.py, filter2.py, filter3.py (Pipe and Filter style).
Fetches and cleans raw stock data, storing it in SQLite.

Homework 2 and 3 
Backend: Homework2/tech_prototype; app.py and technical_analysis.py (Flask + pandas + ta).
Frontend: Homework2/tech_prototype/frontend/ (React).
Main components in src/pages/ (e.g., Home.js, ChartsInfo.js, TechnicalAnalysis.js).
Demo Video: (Stored under Homework3/)


**How to Set Up & Run the Project:**

1. Prerequisites
   - Python 3.x (e.g., Python 3.10 or 3.11)
   - Node.js & npm (e.g., Node 18+)
   - Git (optional, if you’re cloning from a repository)

2. Clone or Download the Project
   git clone https://github.com/YourUser/YourRepo.git
    ( or download the ZIP and extract )
   cd YourRepo
   (Adjust the URL to match your actual GitHub repository.)

3. Run Filters to Populate Databases
     1) Navigate to the filter scripts folder (e.g. Homework1/filters).
     2) Run them
        cd Homework1/filters
        python filter1.py
        ( which will automatically call filter2 and filter3 )
   - This step sets up the DBs with the necessary stock data.

4. Install & Run the Flask Backend
   1) Open a terminal in the folder containing app.py 
   2) Install Python dependencies (Flask, Flask-CORS, pandas, ta, etc.):
      pip install flask flask-cors requests pandas ta beautifulsoup4
   3) Launch the Flask server:
      python app.py
      ( Backend is now running at http://127.0.0.1:5000 (keep this terminal open) )

5. Install & Run the React Frontend
   1) Open another terminal in the frontend folder (Homework2/tech_prototype/frontend)
   2) Install Node modules:
      npm install
       If needed, also run:
      npm install react-financial-charts d3-time-format d3-format recharts
   3) Start the React dev server:
      npm start
      ( Frontend is at http://localhost:3000 )

6. View & Interact with the Application
   - In your browser, visit http://localhost:3000 (frontend).
   - The frontend calls http://127.0.0.1:5000 (Flask backend) for data.

Note:
- If you run into missing dependencies, install them accordingly (e.g., “pip install ...” or “npm install ...”).
- Adjust folder paths in these commands if your project structure differs.


