@echo off

REM Force the working directory to the folder of this .bat file:
cd /d "%~dp0"

echo Starting Filter microservice (port 5001)...
start cmd /k "cd filter_service && python filter_service_app.py"

echo Starting Analysis microservice (port 5002)...
start cmd /k "cd analysis_service && python analysis_service_app.py"

echo Starting Gateway (port 5000)...
start cmd /k "cd gateway && python app.py"

echo All microservices started.
pause
