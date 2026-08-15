from fastapi import FastAPI

from app.api.rules import router as rules_router
from app.api.webhook import router as webhook_router

app = FastAPI(title="LinkPlease")


app.include_router(rules_router)
app.include_router(webhook_router)


@app.get("/")
def root():
    return {"status": "ok"}