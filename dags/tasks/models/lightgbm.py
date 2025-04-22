from airflow.decorators import task
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.metrics import r2_score
import lightgbm as lgb
from lightgbm import early_stopping, log_evaluation
import numpy as np
import joblib
import os
import logging
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

@task
def lightgbm_model(data_file_path, test_file_path):
    output_dir = "/opt/airflow/data/lightgbm_outputs"
    os.makedirs(output_dir, exist_ok=True)
    pdf_path = os.path.join(output_dir, "lightgbm_report.pdf")

    df = pd.read_csv(data_file_path)

    # extract year and month
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['year'] = df['transaction_date'].dt.year
    df['month'] = df['transaction_date'].dt.month
    df['months_since_2001'] = (df['year'] - 2001) * 12 + df['month']

    categorical_cols = ['district', 'type']
    df[categorical_cols] = df[categorical_cols].astype('category')

    # features = [
    #     'storey_range', 'floor_area_sqm',
    #     'lease_commence_year', 'remaining_lease_months',
    #     'district', 'type',
    #     'exchange_rate', 'interest_rate', 'cpi',
    #     'unemployment_rate', 'median_household_income', 'median_individual_income', 
    #     'months_since_2001'
    # ]
    # target = 'price'

    #Removed storey_range
    features = [
        'floor_area_sqm',
        'lease_commence_year', 'remaining_lease_months',
        'district', 'type',
        'exchange_rate', 'interest_rate', 'cpi',
        'unemployment_rate', 'median_household_income', 'median_individual_income', 
        'months_since_2001'
    ]
    target = 'price'

    X = df[features]
    y = df[target]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    lgb_train = lgb.Dataset(X_train, label=y_train, categorical_feature=categorical_cols)
    lgb_eval = lgb.Dataset(X_test, label=y_test, reference=lgb_train)

    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'learning_rate': 0.05,
        'num_leaves': 31,
        'feature_pre_filter': False,
    }

    model = lgb.train(
        params,
        lgb_train,
        valid_sets=[lgb_train, lgb_eval],
        num_boost_round=1000,
        callbacks=[
            early_stopping(stopping_rounds=50),
            log_evaluation(period=100)  # print every 100 iterations
        ]
    )

    joblib.dump(model, "lgb_model_price_per_sqm.pkl")
    joblib.dump(features, "model_features.pkl")

    y_pred = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    logging.info(f"RMSE: {rmse:.2f}")
    logging.info(f"MAE: {mae:.2f}")
    logging.info(f"R² Score: {r2:.4f}")

    df_test = pd.read_csv(test_file_path)
    df_test['transaction_date'] = pd.to_datetime(df_test['transaction_date'])
    df_test['year'] = df_test['transaction_date'].dt.year
    df_test['month'] = df_test['transaction_date'].dt.month
    df_test['months_since_2001'] = (df_test['year'] - 2001) * 12 + df_test['month']

    # Match categorical types
    df_test[categorical_cols] = df_test[categorical_cols].astype('category')

    # Ensure test data has all necessary features
    missing_cols = set(features) - set(df_test.columns)
    if missing_cols:
        logging.warning(f"Missing columns in test data: {missing_cols}")
        # Optionally, add these columns with default values (e.g., NaNs or 0)
        for col in missing_cols:
            df_test[col] = 0  # or np.nan

    X_pg = df_test[features]
    y_pg = df_test[target]

    # Predict and evaluate
    y_pg_pred = model.predict(X_pg)
    rmse_pg = np.sqrt(mean_squared_error(y_pg, y_pg_pred))
    mae_pg = mean_absolute_error(y_pg, y_pg_pred)
    r2_pg = r2_score(y_pg, y_pg_pred)

    logging.info(f"[Test] RMSE: {rmse_pg:.2f}")
    logging.info(f"[Test] MAE: {mae_pg:.2f}")
    logging.info(f"[Test] R² Score: {r2_pg:.4f}")

    with PdfPages(pdf_path) as pdf:
        # --- Plot Actual vs Predicted ---
        plt.figure(figsize=(8, 6))
        plt.scatter(y_pg, y_pg_pred, alpha=0.3, edgecolors='k', linewidths=0.1)
        plt.plot([y_pg.min(), y_pg.max()], [y_pg.min(), y_pg.max()], 'r--')
        plt.xlabel("Actual Price")
        plt.ylabel("Predicted Price")
        plt.title("Actual vs Predicted Price")
        plt.grid(True)
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        # --- Plot RMSE and R² values on a text page ---
        plt.figure(figsize=(8.5, 11))  # A4 page
        plt.axis("off")
        plt.text(0.1, 0.8, "LightGBM Regression Performance on Test Data", fontsize=16, fontweight="bold")
        plt.text(0.1, 0.6, f"RMSE: {rmse_pg:.2f}", fontsize=14)
        plt.text(0.1, 0.5, f"R² Score: {r2_pg:.4f}", fontsize=14)
        plt.text(0.1, 0.4, f"MAE: {mae_pg:.2f}", fontsize=14)
        pdf.savefig()
        plt.close()