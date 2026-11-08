FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY service.py .

USER 1000
EXPOSE 8080 9100
CMD ["python", "-u", "service.py"]
