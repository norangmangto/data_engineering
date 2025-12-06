import duckdb
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import mean_absolute_error, accuracy_score, roc_auc_score
from xgboost import XGBRegressor, XGBClassifier
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import joblib
import os

DB_PATH = "data/processed/weather.duckdb"
MODEL_DIR = "models"

class LSTMModel(nn.Module):
    def __init__(self, input_size, hidden_size=50, num_layers=1):
        super(LSTMModel, self).__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        # x shape: (batch, seq_len, input_size)
        out, _ = self.lstm(x)
        # Take the last time step output
        out = self.fc(out[:, -1, :])
        return out

def train_lstm(X_train, y_train, X_test, y_test, name, epochs=50):
    # Prepare data for LSTM (samples, time_steps, features)
    # We will treat the input features as one time step for simplicity in this hybrid approach,
    # or ideally we should reshape data to have sequence length.
    # Given the current feature set (lag_1), we effectively have a sequence of length 2 (t-1, t).
    # But let's keep it simple: Treat current features as a single vector input to LSTM (seq_len=1).
    # This acts more like a dense network but uses LSTM cells.

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Reshape to (batch, seq_len, input_size)
    X_train_tensor = torch.FloatTensor(X_train_scaled).unsqueeze(1)
    X_test_tensor = torch.FloatTensor(X_test_scaled).unsqueeze(1)
    y_train_tensor = torch.FloatTensor(y_train.values).unsqueeze(1)
    y_test_tensor = torch.FloatTensor(y_test.values).unsqueeze(1)

    model = LSTMModel(input_size=X_train.shape[1])
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

    print(f"Training LSTM for {name}...")
    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()
        outputs = model(X_train_tensor)
        loss = criterion(outputs, y_train_tensor)
        loss.backward()
        optimizer.step()

        if (epoch+1) % 10 == 0:
            print(f"Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}")

    model.eval()
    with torch.no_grad():
        preds = model(X_test_tensor).numpy()
        mae = mean_absolute_error(y_test, preds)
        print(f"LSTM MAE: {mae:.4f}")

    # Save model and scaler
    torch.save(model.state_dict(), os.path.join(MODEL_DIR, f"lstm_{name}.pth"))
    joblib.dump(scaler, os.path.join(MODEL_DIR, f"scaler_{name}.pkl"))

def train_tensorflow(X_train, y_train, X_test, y_test, name, task_type='regression', epochs=50):
    """Train a TensorFlow/Keras model"""

    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Build model
    model = keras.Sequential([
        layers.Dense(64, activation='relu', input_shape=(X_train.shape[1],)),
        layers.Dropout(0.2),
        layers.Dense(32, activation='relu'),
        layers.Dropout(0.2),
        layers.Dense(16, activation='relu'),
        layers.Dense(1, activation='sigmoid' if task_type == 'classification' else 'linear')
    ])

    if task_type == 'regression':
        model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    else:
        model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy', 'AUC'])

    print(f"Training TensorFlow model for {name}...")

    # Train with early stopping
    early_stop = keras.callbacks.EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

    history = model.fit(
        X_train_scaled, y_train,
        validation_split=0.2,
        epochs=epochs,
        batch_size=32,
        callbacks=[early_stop],
        verbose=0
    )

    # Evaluate
    y_pred = model.predict(X_test_scaled, verbose=0).flatten()

    if task_type == 'regression':
        mae = mean_absolute_error(y_test, y_pred)
        print(f"TensorFlow MAE: {mae:.4f}")
    else:
        y_pred_class = (y_pred > 0.5).astype(int)
        acc = accuracy_score(y_test, y_pred_class)
        auc = roc_auc_score(y_test, y_pred)
        print(f"TensorFlow Accuracy: {acc:.4f}, AUC: {auc:.4f}")

    # Save model
    model.save(os.path.join(MODEL_DIR, f"tf_{name}.keras"))
    joblib.dump(scaler, os.path.join(MODEL_DIR, f"scaler_tf_{name}.pkl"))

def train_model():
    print("Loading data from DuckDB...")
    con = duckdb.connect(DB_PATH)
    df = con.execute("SELECT * FROM int_weather_features").fetch_df()
    con.close()

    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()

    feature_cols = [
        'temp_mean', 'temp_max', 'temp_min',
        'wind_speed', 'humidity', 'precipitation', 'sunshine', 'pressure_surface',
        'month', 'day_of_year',
        'temp_mean_lag_1', 'temp_max_lag_1', 'temp_min_lag_1',
        'pressure_surface_lag_1'
    ]

    feature_cols = [c for c in feature_cols if c in df.columns]
    print(f"Features: {feature_cols}")

    targets = {}
    for i in range(1, 8):
        targets[f'temp_min_day_{i}'] = (f'target_temp_min_day_{i}', 'regression')
        targets[f'temp_max_day_{i}'] = (f'target_temp_max_day_{i}', 'regression')
        targets[f'wind_speed_day_{i}'] = (f'target_wind_speed_day_{i}', 'regression')
        targets[f'humidity_day_{i}'] = (f'target_humidity_day_{i}', 'regression')
        targets[f'rain_prob_day_{i}'] = (f'target_is_raining_day_{i}', 'classification')

    os.makedirs(MODEL_DIR, exist_ok=True)

    for name, (target_col, task_type) in targets.items():
        if target_col not in df.columns:
            continue

        y = df[target_col]
        X = df[feature_cols]

        valid_idx = X.notna().all(axis=1) & y.notna()
        X_valid = X[valid_idx]
        y_valid = y[valid_idx]

        # Split
        split_idx = int(len(X_valid)*0.8)
        X_train, X_test = X_valid.iloc[:split_idx], X_valid.iloc[split_idx:]
        y_train, y_test = y_valid.iloc[:split_idx], y_valid.iloc[split_idx:]

        print(f"\nTraining {name} ({task_type})...")

        if task_type == 'regression':
            # XGBoost
            xgb = XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
            xgb.fit(X_train, y_train)
            preds = xgb.predict(X_test)
            mae = mean_absolute_error(y_test, preds)
            print(f"XGBoost MAE: {mae:.4f}")
            joblib.dump(xgb, os.path.join(MODEL_DIR, f"xgb_{name}.pkl"))

            # LSTM
            train_lstm(X_train, y_train, X_test, y_test, name)

            # TensorFlow
            train_tensorflow(X_train, y_train, X_test, y_test, name, task_type='regression')

        else:
            # Classification (Rain)
            xgb = XGBClassifier(n_estimators=100, learning_rate=0.1, random_state=42)
            xgb.fit(X_train, y_train)
            preds = xgb.predict(X_test)
            probs = xgb.predict_proba(X_test)[:, 1]
            acc = accuracy_score(y_test, preds)
            auc = roc_auc_score(y_test, probs)
            print(f"XGBoost Accuracy: {acc:.4f}, AUC: {auc:.4f}")
            joblib.dump(xgb, os.path.join(MODEL_DIR, f"xgb_{name}.pkl"))

            # TensorFlow
            train_tensorflow(X_train, y_train, X_test, y_test, name, task_type='classification')

if __name__ == "__main__":
    train_model()
