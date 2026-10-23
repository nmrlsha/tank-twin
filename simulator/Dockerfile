FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator.py .

# Run as a normal user, not root.
RUN useradd --create-home appuser
USER appuser

CMD ["python", "simulator.py"]
