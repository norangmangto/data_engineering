from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb
import pandas as pd
import joblib
import torch
import numpy as np
from typing import Optional, Dict, List
import os

app = FastAPI(title="DWD Weather Prediction API")

# Paths
DB_PATH = "data/processed/weather.duckdb"
MODEL_DIR = "models"

# Load models on startup
models = {}

@app.on_event("startup")
async def load_models():
    """Load all trained models into memory"""
    print("Loading models...")

    # Load XGBoost models for 7 days
    targets = ['temp_min', 'temp_max', 'wind_speed', 'humidity', 'rain_prob']
    for i in range(1, 8):
        for target in targets:
            name = f"{target}_day_{i}"
            model_path = os.path.join(MODEL_DIR, f"xgb_{name}.pkl")
            if os.path.exists(model_path):
                models[f"xgb_{name}"] = joblib.load(model_path)
                print(f"Loaded XGBoost model for {name}")

    print(f"Loaded {len(models)} models")

class DailyPrediction(BaseModel):
    date: str
    temp_min: float
    temp_max: float
    wind_speed: float
    humidity: float
    rain_prob: float

class ForecastResponse(BaseModel):
    forecast: List[DailyPrediction]
    model_type: str = "xgboost"

@app.get("/")
async def root():
    return {
        "message": "DWD Weather Prediction API",
        "endpoints": {
            "/predict": "Get next day weather prediction",
            "/health": "Health check"
        }
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "models_loaded": len(models)}

@app.get("/predict", response_model=ForecastResponse)
async def predict():
    """
    Predict weather for the next 7 days using the latest data from the database.
    """
    try:
        # Get latest data
        con = duckdb.connect(DB_PATH)
        query = """
        SELECT * FROM int_weather_features
        ORDER BY date DESC
        LIMIT 1
        """
        df = con.execute(query).fetch_df()
        con.close()

        if df.empty:
            raise HTTPException(status_code=404, detail="No data available")

        # Prepare features
        feature_cols = [
            'temp_mean', 'temp_max', 'temp_min',
            'wind_speed', 'humidity', 'precipitation', 'sunshine', 'pressure_surface',
            'month', 'day_of_year',
            'temp_mean_lag_1', 'temp_max_lag_1', 'temp_min_lag_1',
            'pressure_surface_lag_1'
        ]

        feature_cols = [c for c in feature_cols if c in df.columns]
        X = df[feature_cols].iloc[0:1]  # Single row

        # Make predictions for 7 days
        forecast = []
        latest_date = pd.to_datetime(df['date'].iloc[0])

        for i in range(1, 8):
            day_preds = {}
            for target in ['temp_min', 'temp_max', 'wind_speed', 'humidity', 'rain_prob']:
                name = f"{target}_day_{i}"
                model_key = f"xgb_{name}"

                val = 0.0
                if model_key in models:
                    pred = models[model_key].predict(X)[0]
                    # For rain_prob, get probability
                    if target == 'rain_prob':
                        pred = models[model_key].predict_proba(X)[0][1]
                    val = float(pred)

                day_preds[target] = val

            next_date = latest_date + pd.Timedelta(days=i)

            forecast.append(DailyPrediction(
                date=next_date.strftime("%Y-%m-%d"),
                temp_min=day_preds.get('temp_min', 0.0),
                temp_max=day_preds.get('temp_max', 0.0),
                wind_speed=day_preds.get('wind_speed', 0.0),
                humidity=day_preds.get('humidity', 0.0),
                rain_prob=day_preds.get('rain_prob', 0.0)
            ))

        return ForecastResponse(
            forecast=forecast,
            model_type="xgboost"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
