from fastapi import FastAPI

app = FastAPI(
    title="Retail Data API",
    description="Event-driven retail analytics with Kafka streaming",
    version="0.1.0"
)

@app.get("/health")
def health():
    return {"status": "ok", "service": "api"}