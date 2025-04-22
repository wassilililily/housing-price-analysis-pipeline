from airflow.decorators import task
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import logging
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from matplotlib.backends.backend_pdf import PdfPages

@task
def randomforest_model(data_file_path, test_file_path):
    output_dir = "/opt/airflow/data/rf_outputs"
    os.makedirs(output_dir, exist_ok=True)
    pdf_path = os.path.join(output_dir, "rf_test_report.pdf")

    # Import and inspect data
    df = pd.read_csv(data_file_path)

    # Drop null values
    df.dropna(inplace=True)

    # Feature engineering: Add column "months_since_2001" and drop unused columns
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    df['months_since_2001'] = (df['transaction_date'].dt.year - 2001) * 12 + df['transaction_date'].dt.month
    df['storey_range'] = df['storey_range'].astype(int)
    df['log_price'] = np.log(df['price'])
    df.drop(columns=[
        'id', 
        'transaction_date', 
        'price_per_sqm',
        'price'
    ], errors='ignore', inplace=True)

    #drop storey_range
    df = df.drop(columns=['storey_range'])

    # Define dependent variable
    X = df.drop(columns=['log_price'])
    # Define independent variable
    y = df['log_price']

    # Categorical and numeric columns
    categorical = ['type', 'district']
    numeric = [col for col in X.columns if col not in categorical]
    # Preprocessing
    ohe = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
    ohe.set_output(transform='default')
    preprocessor = ColumnTransformer([
        ('cat', ohe, categorical)
    ], remainder='passthrough')

    # Pipeline
    pipeline = Pipeline([
        ('preprocessing', preprocessor),
        ('model', RandomForestRegressor(random_state=42))
    ])

    # Train with 80/20 train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    pipeline.fit(X_train, y_train)
    # Predict
    y_pred = pipeline.predict(X_test)
    y_pred_exp = np.exp(y_pred)

    # Evaluation
    y_test_exp = np.exp(y_test)
    mse = mean_squared_error(y_test_exp, y_pred_exp)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test_exp, y_pred_exp)

    logging.info(f'RMSE: {rmse:.2f}')
    logging.info(f'R^2: {r2:.2f}')

    # Setting up PG data as test of model
    df_test = pd.read_csv(test_file_path)
    df_test['transaction_date'] = pd.to_datetime(df_test['transaction_date'], errors='coerce')
    df_test['months_since_2001'] = (df_test['transaction_date'].dt.year - 2001) * 12 + df_test['transaction_date'].dt.month
    df_test['log_price'] = np.log(df_test['price'])

    df_test.drop(columns=[
        'id', 
        'transaction_date', 
        'price_per_sqm',
        'price'
    ], errors='ignore', inplace=True)

    # Make sure df_test has the same columns as X (used to train)
    missing_cols = set(X.columns) - set(df_test.columns)
    if missing_cols:
        logging.warning(f"Missing columns in df_test: {missing_cols}")

    # Define test features and labels
    X_pg = df_test.drop(columns=['log_price'])
    y_pg = df_test['log_price']

    # Predict using the pipeline
    y_pg_pred = pipeline.predict(X_pg)
    y_pg_pred_exp = np.exp(y_pg_pred)
    y_pg_true_exp = np.exp(y_pg)

    # Evaluation on df_test
    mse_pg = mean_squared_error(y_pg_true_exp, y_pg_pred_exp)
    rmse_pg = np.sqrt(mse_pg)
    r2_pg = r2_score(y_pg_true_exp, y_pg_pred_exp)

    logging.info(f'[PG] RMSE: {rmse_pg:.2f}')
    logging.info(f'[PG] R^2: {r2_pg:.2f}')

    with PdfPages(pdf_path) as pdf:
        # 1. Plot Actual vs Predicted Prices
        plt.figure(figsize=(10, 6))
        plt.scatter(y_pg_true_exp, y_pg_pred_exp, alpha=0.3, color='teal', label='Predicted vs Actual')
        plt.plot([y_pg_true_exp.min(), y_pg_true_exp.max()],
                 [y_pg_true_exp.min(), y_pg_true_exp.max()],
                 'r--', lw=2, label='Ideal Prediction Line')
        plt.xlabel("Actual Price")
        plt.ylabel("Predicted Price")
        plt.title("Random Forest: Actual vs Predicted Prices (Test Set)")
        plt.legend()
        plt.grid(True)
        pdf.savefig()
        plt.close()

        # 2. Add Metrics as Text Page
        plt.figure(figsize=(8.5, 11))
        plt.axis('off')
        metrics_text = (
            f"Random Forest Evaluation on PG Test Set\n\n"
            f"RMSE: {rmse_pg:,.2f}\n"
            f"R² Score: {r2_pg:.4f}\n\n"
            f"Model was trained on {len(df)} samples\n"
            f"Tested on {len(df_test)} samples"
        )
        plt.text(0.1, 0.8, metrics_text, fontsize=12, va='top')
        pdf.savefig()
        plt.close()