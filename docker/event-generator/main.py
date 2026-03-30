from fastapi import FastAPI

from routers.order_events import router as order_events_router
from routers.clickstream import router as clickstream_router
from routers.footfall import router as footfall_router

app = FastAPI(title="Event Generator Service")


app.include_router(order_events_router, prefix="/events/orders", tags=["orders"])
app.include_router(clickstream_router, prefix="/events/clickstream", tags=["clickstream"])
app.include_router(footfall_router, prefix="/events/footfall", tags=["footfall"])


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "event-generator"
    }