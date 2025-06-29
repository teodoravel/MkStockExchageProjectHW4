FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY Homework4/analysis_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY Homework4/analysis_service/ /app

EXPOSE 5000
CMD ["python", "analysis_service_app.py"]
