from airflow.decorators import task
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from sklearn.metrics import mean_squared_error, r2_score
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
import logging

@task
def arima_model(data_file_path, test_file_path):
    output_dir = "/opt/airflow/data/arima_outputs"
    os.makedirs(output_dir, exist_ok=True)
    pdf_path = os.path.join(output_dir, "arima_report.pdf")

    df = pd.read_csv(data_file_path)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    exog_cols = [
        'exchange_rate', 'interest_rate', 'cpi', 'unemployment_rate',
        'median_household_income', 'median_individual_income',
        'remaining_lease_months', 'floor_area_sqm'
    ]

    with PdfPages(pdf_path) as pdf:
        # ==== ARIMA ====
        monthly_price = df.set_index('transaction_date')['price'].resample('M').mean().dropna()
        log_price = np.log1p(monthly_price)
        log_price_diff = log_price.diff().dropna()

        split_index = int(len(log_price_diff) * 0.8)
        y_train, y_test = log_price_diff[:split_index], log_price_diff[split_index:]
        model = ARIMA(y_train, order=(1, 0, 1))
        model_fit = model.fit()
        pred_diff = model_fit.forecast(steps=len(y_test))
        last_train_log = log_price.iloc[split_index - 1]
        pred_log = last_train_log + np.cumsum(pred_diff)
        forecast = np.expm1(pred_log)
        actual = np.expm1(log_price[split_index:split_index + len(pred_log)])
        rmse = np.sqrt(mean_squared_error(actual, forecast))
        r2 = r2_score(actual, forecast)

        # Plot ARIMA
        plt.figure(figsize=(12, 6))
        plt.plot(actual.index, actual, label='Actual')
        plt.plot(forecast.index, forecast, label='ARIMA Forecast', linestyle='--')
        plt.title(f"ARIMA Forecast | RMSE={rmse:.2f}, R²={r2:.4f}")
        plt.xlabel("Date")
        plt.ylabel("Price per sqm")
        plt.legend()
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        # Save ARIMA Metrics
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.axis('off')
        ax.text(0.01, 0.6, f"ARIMA Model:\nRMSE = {rmse:.2f}\nR² Score = {r2:.4f}", fontsize=12)
        pdf.savefig()
        plt.close()

        # ==== ARIMAX by district ====
        results = []
        for district, group_df in df.groupby("district"):
            try:
                group_df = group_df.set_index("transaction_date").resample("M").mean(numeric_only=True)
                if group_df.shape[0] < 24:
                    continue

                log_price = np.log1p(group_df["price"])
                log_diff = log_price.diff().dropna()
                exog = group_df[exog_cols].loc[log_diff.index].dropna()
                log_diff = log_diff.loc[exog.index]  # ensure alignment

                if len(log_diff) < 24:
                    continue

                split_idx = int(len(log_diff) * 0.8)
                y_train, y_test = log_diff[:split_idx], log_diff[split_idx:]
                X_train, X_test = exog[:split_idx], exog[split_idx:]

                model = SARIMAX(y_train, exog=X_train, order=(1, 0, 1))
                model_fit = model.fit(disp=False)
                pred_diff = model_fit.forecast(steps=len(y_test), exog=X_test)
                last_log = log_price.loc[y_test.index[0] - pd.offsets.MonthBegin(1)]
                pred_log = last_log + np.cumsum(pred_diff)
                forecast = np.expm1(pred_log)
                actual = np.expm1(log_price[y_test.index])

                r2 = r2_score(actual, forecast)
                rmse = np.sqrt(mean_squared_error(actual, forecast))
                results.append({'district': district, 'r2': r2, 'rmse': rmse})

                # Plot ARIMAX
                plt.figure(figsize=(10, 5))
                plt.plot(actual.index, actual, label='Actual')
                plt.plot(forecast.index, forecast, label='ARIMAX Forecast')
                plt.title(f"District: {district} | RMSE={rmse:.2f}, R²={r2:.4f}")
                plt.legend()
                plt.tight_layout()
                pdf.savefig()
                plt.close()

            except Exception as e:
                logging.warning(f"Failed for district {district}: {e}")
                continue

        # Save ARIMAX Summary Table
        if results:
            results_df = pd.DataFrame(results).sort_values('r2', ascending=False)
            fig, ax = plt.subplots(figsize=(10, 0.4 * len(results_df) + 1))
            ax.axis('off')
            table_text = results_df.round(3).to_string(index=False)
            ax.text(0, 1, "ARIMAX Model Summary (by District)", fontsize=14, weight='bold')
            ax.text(0, 0.95, table_text, fontsize=10, family='monospace')
            pdf.savefig()
            plt.close()
