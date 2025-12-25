FROM prefecthq/prefect:2.19.3-python3.11

WORKDIR /workspace

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc python3-dev libpq-dev \
    && rm -rf /var/lib/apt/lists/*
	
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt