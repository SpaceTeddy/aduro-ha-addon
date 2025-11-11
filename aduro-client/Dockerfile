FROM python:3.12-slim

# Systempackages (for Requests/cryptography & jq)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl jq ca-certificates build-essential libffi-dev libssl-dev \
 && rm -rf /var/lib/apt/lists/*

# Pip Tooling update
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel

# Python-dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# App
WORKDIR /app
COPY main.py /app/main.py
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

CMD ["/app/run.sh"]
