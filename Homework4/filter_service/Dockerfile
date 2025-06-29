# ---- BASE IMAGE -------------------------------------------------
FROM python:3.11-slim AS base

# ---- ENVIRONMENT ------------------------------------------------
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# ---- SYSTEM DEPENDENCIES ---------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

# ---- WORKDIR ----------------------------------------------------
WORKDIR /app

# ---- COPY REQUIREMENTS & INSTALL --------------------------------
COPY Homework4/filter_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ---- COPY SOURCE ------------------------------------------------
COPY Homework4/filter_service/ /app

# ---- EXPOSE & CMD ----------------------------------------------
EXPOSE 5001
CMD ["python", "filter_service_app.py"]
