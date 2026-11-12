FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml cluster.yaml ./
COPY proto proto
COPY dynamo dynamo
COPY client client
RUN python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/dynamo/dynamo.proto

# DATA_DIR must be the mounted folder, or the data is lost with the container.
# PYTHONUNBUFFERED makes `docker compose logs` show lines as they are written.
ENV CONFIG=cluster.yaml DATA_DIR=/data PYTHONUNBUFFERED=1

CMD ["python", "-m", "dynamo.server"]
