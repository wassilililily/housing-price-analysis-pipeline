from airflow.decorators import task
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import xgboost as xgb
import os
import logging
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from matplotlib.backends.backend_pdf import PdfPages

@task
def xgboost_model(data_file_path, test_file_path):
    output_dir = "/opt/airflow/data/xgboost_outputs"
    os.makedirs(output_dir, exist_ok=True)
    pdf_path = os.path.join(output_dir, "xgboost_report.pdf")

    try:
        logging.info(f"Loading training data from: {data_file_path}")
        df = pd.read_csv(data_file_path)
        logging.info(f"Loading test data from: {test_file_path}")
        df_test = pd.read_csv(test_file_path)

        # Transform log(price)
        df['price'] = np.log(df['price'])
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])

        for col in ['year', 'month', 'day', 'day_of_week', 'quarter']:
            df[col] = getattr(df['transaction_date'].dt, col)

        df['months_since_2001'] = (df['transaction_date'].dt.year - 2001) * 12 + df['transaction_date'].dt.month

        # Drop unnecessary columns
        df_clean = df.drop(columns=['transaction_date', 'id', 'price_per_sqm'])
        model_data = pd.get_dummies(df_clean)
        X = model_data.drop(columns=['price'])
        y = model_data['price']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = xgb.XGBRegressor(
            objective="reg:squarederror",
            random_state=42,
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            colsample_bytree=0.8,
            n_jobs=-1
        )
        model.fit(X_train, y_train)

        importance_df = pd.DataFrame({
            'Feature': X_train.columns,
            'Importance': model.feature_importances_
        }).sort_values(by='Importance', ascending=False)

        top_features = importance_df.head(10)['Feature'].values

        model.fit(X_train[top_features], y_train)
        y_pred = model.predict(X_test[top_features])
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)
        r2 = r2_score(y_test, y_pred)

        logging.info(f'MSE: {mse}')
        logging.info(f'RMSE: {rmse}')
        logging.info(f'R^2: {r2}')

        # Prepare test data
        df_test['price'] = np.log(df_test['price'])
        df_test['transaction_date'] = pd.to_datetime(df_test['transaction_date'])

        for col in ['year', 'month', 'day', 'day_of_week', 'quarter']:
            df_test[col] = getattr(df_test['transaction_date'].dt, col)

        df_test['months_since_2001'] = (df_test['transaction_date'].dt.year - 2001) * 12 + df_test['transaction_date'].dt.month

        df_test_encoded = pd.get_dummies(df_test.drop(columns=['transaction_date', 'id', 'price_per_sqm']))
        val_data = df_test_encoded.reindex(columns=X_train.columns, fill_value=0)
        test_data_important = val_data[top_features]
        y_val = df_test['price']

        y_test_pred = model.predict(test_data_important)
        mse_val = mean_squared_error(y_val, y_test_pred)
        rmse_val = np.sqrt(mse_val)
        r2_val = r2_score(y_val, y_test_pred)

        logging.info(f'MSE_Val: {mse_val}')
        logging.info(f'RMSE_Val: {rmse_val}')
        logging.info(f'R^2_Val: {r2_val}')

        with PdfPages(pdf_path) as pdf:

            # Page 1: Feature importance
            plt.figure(figsize=(10, 6))
            sns.barplot(x="Importance", y="Feature", data=importance_df.head(10))
            plt.title("Top 10 Feature Importances")
            plt.tight_layout()
            pdf.savefig()
            plt.close()

            # Page 2: Actual vs predicted
            plt.figure(figsize=(8, 6))
            sns.scatterplot(x=y_val, y=y_test_pred, alpha=0.4)
            plt.xlabel('Actual Log')
            plt.ylabel('Predicted Log')
            plt.title('Actual vs Predicted')
            plt.plot([y_val.min(), y_val.max()], [y_val.min(), y_val.max()], 'r--')
            plt.tight_layout()
            pdf.savefig()
            plt.close()

            # Page 3: Metrics
            plt.figure(figsize=(8.5, 11))
            plt.axis('off')
            metrics_text = (
                "XGBoost Model Metrics\n\n"
                "Training:\n"
                f"    MSE:  {mse:.4f}\n"
                f"    RMSE: {rmse:.4f}\n"
                f"    R2:   {r2:.4f}\n\n"
                "Validation:\n"
                f"    MSE:  {mse_val:.4f}\n"
                f"    RMSE: {rmse_val:.4f}\n"
                f"    R2:   {r2_val:.4f}\n"
            )
            plt.text(0.01, 0.95, metrics_text, fontsize=12, verticalalignment='top', family='monospace')
            pdf.savefig()
            plt.close()

        logging.info(f"Model outputs saved in: {pdf_path}")

    except Exception as e:
        logging.error(f"XGBoost pipeline failed: {e}")
        raise
