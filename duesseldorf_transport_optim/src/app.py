from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from router import router
import uvicorn

app = FastAPI(title="Düsseldorf Public Transport Optimizer")

class GeoPoint(BaseModel):
    lat: float
    lon: float

class RouteRequest(BaseModel):
    start: GeoPoint
    destination: GeoPoint
    # time: str  # Not used in this static version yet

@app.on_event("startup")
def startup_event():
    router.load_graph()

@app.get("/health")
def health():
    return {"status": "ok", "nodes_loaded": len(router.stops)}

@app.post("/route")
def get_optimal_route(request: RouteRequest):
    """
    Find optimal route avoiding accident hotspots.
    """
    try:
        route = router.get_route(
            request.start.lat, request.start.lon,
            request.destination.lat, request.destination.lon
        )

        if not route:
            raise HTTPException(status_code=404, detail="No route found or stops too far.")

        return route

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
