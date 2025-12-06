# Data Engineering Workspace

This repository contains data engineering projects focusing on pipelines, databases, and machine learning integration.

## Projects

### 1. [DWD Weather Prediction](./dwd_weather_prediction)
A continuously updating data pipeline that fetches weather data from the German Meteorological Service (DWD) for Düsseldorf. It processes data using **DuckDB** and **dbt**, and trains **XGBoost**, **LSTM**, and **TensorFlow** models to provide a 7-day forecast for temperature, wind, and rain via a **FastAPI** endpoint.

### 2. [Düsseldorf Transport Optimizer](./duesseldorf_transport_optim)
A routing application that finds the safest and fastest public transport routes in Düsseldorf. It combines **GTFS** (Timetables), **Accident Data** (Unfallatlas), and **Traffic Data** to calculate "Risk Scores" for transport stops. It uses a custom **NetworkX** graph router to penalize high-risk areas.

## Technologies
- **Language**: Python 3.13 (managed by `uv`)
- **Database**: DuckDB
- **Transformation**: dbt (data build tool)
- **Machine Learning**: PyTorch, TensorFlow/Keras, XGBoost, Scikit-Learn
- **API**: FastAPI
- **Orchestration**: Custom Scripts
