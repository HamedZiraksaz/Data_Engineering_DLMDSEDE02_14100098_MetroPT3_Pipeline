FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard.py .

EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "dashboard.py", "--server.address=0.0.0.0"]
