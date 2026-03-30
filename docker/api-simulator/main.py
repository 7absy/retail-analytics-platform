from fastapi import FastAPI
from routers.orders import router as orders_router
from routers.campaigns import router as campaigns_router

app = FastAPI(title="API Simulator")

# mount routers correctly
app.include_router(orders_router, prefix="/orders")
app.include_router(campaigns_router, prefix="/campaigns")


@app.get("/health")
def health_check():
    return {"status": "ok"}