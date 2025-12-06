# DWD Weather Prediction with 7-Day Forecast

A data engineering and ML pipeline that predicts weather for Düsseldorf using historical data from the DWD (Deutscher Wetterdienst).

## Features
- **Continuous Ingestion**: Fetches daily weather data (historical + decent) from DWD Open Data.
- **Modern Data Stack**: Uses **DuckDB** for storage and **dbt** for feature engineering (lag features, window functions).
- **Advanced Modeling**: Trains multiple models:
  - **XGBoost**: For robust tabular regression/classification.
  - **LSTM (PyTorch)**: Time-series forecasting.
  - **TensorFlow/Keras**: Deep learning baseline.
- **7-Day Forecasting**: Predicts min/max temp, wind speed, humidity, and rain probability for the next 7 days.
- **API**: Serves predictions via a FastAPI endpoint.

## Setup

1. **Install Dependencies**
   ```bash
   uv sync
   ```

2. **Ingest Data**
   Fetches raw data and saves to `data/raw`.
   ```bash
   uv run python src/ingestion.py
   ```

3. **Transform Data**
   Builds the `int_weather_features` table with 7-day targets.
   ```bash
   cd transform
   uv run dbt run
   cd ..
   ```

4. **Train Models**
   Trains 35 models (5 targets * 7 days). This may take a while.
   ```bash
   uv run python src/train.py
   ```

5. **Run API**
   Starts the FastAPI server on port 8000.
   ```bash
   # Using the start script
   sh start_api.sh
   # OR directly
   uv run uvicorn src.app:app
   ```

## API Usage

**Get 7-Day Forecast:**
```bash
curl http://localhost:8000/predict
```

**Response Example:**
```json
{
  "forecast": [
    {
      "date": "2025-12-07",
      "temp_min": 3.5,
      "temp_max": 8.2,
      "wind_speed": 4.1,
      "humidity": 78.0,
      "rain_prob": 0.15
    },
    ...
  ],
  "model_type": "xgboost"
}
```
