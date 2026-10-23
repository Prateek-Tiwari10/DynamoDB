FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN bash scripts/gen_proto.sh && pip install --no-cache-dir -e .

ENV PYTHONUNBUFFERED=1
EXPOSE 50051

CMD ["python", "-m", "dynamo.server"]
