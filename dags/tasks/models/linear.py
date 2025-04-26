from airflow.decorators import task
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import logging
from matplotlib.backends.backend_pdf import PdfPages

from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_squared_error, r2_score

@task
def linear_models(data_file_path, test_file_path):
    output_dir = "/opt/airflow/data/linear_outputs"
    os.makedirs(output_dir, exist_ok=True)

    try:
        df = pd.read_csv(data_file_path)
        test = pd.read_csv(test_file_path)
        min_date = pd.to_datetime(df['transaction_date']).min()

        for dataset in [df, test]:
            dataset['transaction_date'] = pd.to_datetime(dataset['transaction_date'], errors='coerce')
            dataset['year'] = dataset['transaction_date'].dt.year
            dataset['month'] = dataset['transaction_date'].dt.month
            dataset['months_since_min'] = (dataset['year'] - min_date.year) * 12 + (dataset['month'] - min_date.month)
            dataset['property_age'] = dataset['year'] - dataset['lease_commence_year']
            dataset['avg_storey'] = dataset['storey_range'].astype(float)

            dataset['price'] = dataset['price'].clip(lower=1)
            dataset['floor_area_sqm'] = dataset['floor_area_sqm'].clip(lower=1)
            dataset['remaining_lease_months'] = dataset['remaining_lease_months'].clip(lower=1)
            dataset['log_price'] = np.log1p(dataset['price'])

            dataset['lease_ratio'] = dataset['remaining_lease_months'] / (99 * 12)
            dataset['income_per_sqm'] = dataset['median_household_income'] / dataset['floor_area_sqm']

            dataset['month_sin'] = np.sin(2 * np.pi * dataset['month'] / 12)
            dataset['month_cos'] = np.cos(2 * np.pi * dataset['month'] / 12)

            for col in dataset.select_dtypes(include=[np.number]).columns:
                dataset[col] = dataset[col].replace([np.inf, -np.inf], np.nan).fillna(dataset[col].median())

        region_stats = df.groupby('district')['log_price'].agg(['mean', 'median']).reset_index()
        region_stats.columns = ['district', 'district_avg_log_price', 'district_median_log_price']
        df = df.merge(region_stats, on='district', how='left')
        test = test.merge(region_stats, on='district', how='left')
        train = df

        features = [
            'floor_area_sqm', 'avg_storey', 'property_age',
            'remaining_lease_months', 'lease_ratio', 'months_since_min',           
            'month_sin', 'month_cos',
            'district', 'type'
        ]
        categorical_features = ['district', 'type', 'age_bin', 'area_bin']
        numeric_features = [f for f in features if f not in categorical_features]

        X_train = train[features]
        y_train = train['log_price']
        X_test = test[features]
        y_test_log = test['log_price']
        y_test = np.expm1(y_test_log)

        numeric_transformer = Pipeline([
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
        ])
        categorical_transformer = Pipeline([
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
        ])
        preprocessor = ColumnTransformer([
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)
        ])

        models = {
            "LinearRegression": LinearRegression(),
            "Ridge": Ridge(alpha=1.0),
            "Lasso": Lasso(alpha=0.001),
            "ElasticNet": ElasticNet(alpha=0.001, l1_ratio=0.5)
        }

        for name, model in models.items():
            pipeline = Pipeline([
                ('preprocessor', preprocessor),
                ('model', model)
            ])
            pipeline.fit(X_train, y_train)

            y_pred_log = pipeline.predict(X_test)
            y_pred_log = np.maximum(y_pred_log, 0)
            y_pred = np.expm1(y_pred_log)

            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            r2 = r2_score(y_test, y_pred)
            r2_log = r2_score(y_test_log, y_pred_log)

            pdf_path = os.path.join(output_dir, f"{name.lower()}_final_report.pdf")
            with PdfPages(pdf_path) as pdf:
                plt.figure(figsize=(8, 6))
                plt.scatter(y_test_log, y_pred_log, alpha=0.4, color='skyblue', edgecolors='k', linewidths=0.3)
                plt.plot([y_test_log.min(), y_test_log.max()], [y_test_log.min(), y_test_log.max()], 'r--')
                plt.xlabel("Actual Log Price")
                plt.ylabel("Predicted Log Price")
                plt.title(f"{name} | Actual vs Predicted (Log Scale)")
                plt.grid(True)
                plt.tight_layout()
                pdf.savefig()
                plt.close()

                plt.figure(figsize=(8.5, 11))
                plt.axis('off')
                plt.text(0.1, 0.5, f"""{name} Final Model Metrics

RMSE (actual):    {rmse:,.2f}
R² (actual):      {r2:.4f}
R² (log):         {r2_log:.4f}

""", fontsize=14, verticalalignment='center', fontweight='bold')
                pdf.savefig()
                plt.close()

            logging.info(f"{name} Done | RMSE: {rmse:.2f}, R²: {r2:.4f}, R²_log: {r2_log:.4f}")
            logging.info(f"Report saved to: {pdf_path}")

    except Exception as e:
        logging.error(f"Linear model pipeline failed: {e}")
        raise
