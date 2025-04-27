from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np
import pandas as pd
import os

current_dir = os.path.dirname(os.path.abspath(__file__))

model_path = os.path.join(current_dir, "data", "xgboost_lgbm_outputs", "xgboost_lgbm_pipeline.pkl")

pipeline = joblib.load(model_path)

app = FastAPI()

class InputData(BaseModel):
    floor_area_sqm: float
    avg_storey: float
    property_age: float
    property_age_squared: float
    remaining_lease_months: float
    lease_ratio: float
    median_household_income: float
    income_per_sqm: float
    income_lease_interaction: float
    income_x_area: float
    income_x_age: float
    area_x_age: float
    storey_x_age: float
    log_price_per_sqm: float
    months_since_min: int
    year: int
    month_sin: float
    month_cos: float
    district_avg_log_price: float
    district_median_log_price: float
    district: str
    type: str
    age_bin: int
    area_bin: int

@app.post("/predict")
def predict(data: InputData):
    input_df = pd.DataFrame([data.dict()])
    
    pred_log_price = pipeline.predict(input_df)
    pred_price = np.expm1(np.maximum(pred_log_price, 0))  # undo the log1p

    return {"predicted_price": pred_price.tolist()[0]}
