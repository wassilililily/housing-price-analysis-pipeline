from airflow.decorators import task
import pandas as pd
import numpy as np
import os
import logging
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

import seaborn as sns
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV

import warnings
warnings.filterwarnings('ignore')

def extract_storey_numeric(storey_range):
    try:
        return float(storey_range)
    except:
        return np.nan
    
# Evaluation
def compute_aic_bic(y_true, y_pred, k):
    n = len(y_true)
    rss = np.sum((y_true - y_pred) ** 2)
    aic = n * np.log(rss / n) + 2 * k
    bic = n * np.log(rss / n) + k * np.log(n)
    return aic, bic

def check_and_fix_data(X, y):
    X_copy = X.copy()
    y_copy = y.copy()
    for col in X_copy.columns:
        X_copy[col] = X_copy[col].replace([np.inf, -np.inf], np.nan)
    for col in X_copy.select_dtypes(include=[np.number]).columns:
        q999 = X_copy[col].quantile(0.999)
        q001 = X_copy[col].quantile(0.001)
        X_copy.loc[X_copy[col] > q999, col] = q999
        X_copy.loc[X_copy[col] < q001, col] = q001
        X_copy[col] = X_copy[col].fillna(X_copy[col].median())
    return X_copy, y_copy


@task
def linear_regression(data_file_path, test_file_path):
    output_dir = "/opt/airflow/data/linear_regression_outputs"
    os.makedirs(output_dir, exist_ok=True)
    pdf_path = os.path.join(output_dir, "linear_regression_report.pdf")

    train_df = pd.read_csv(data_file_path)

    train_df['transaction_date'] = pd.to_datetime(train_df['transaction_date'])
    train_df = train_df[train_df['transaction_date'].dt.year >= 2002]

    # Time-based features
    train_df['year'] = train_df['transaction_date'].dt.year
    train_df['month'] = train_df['transaction_date'].dt.month
    train_df['quarter'] = train_df['transaction_date'].dt.quarter
    train_df['year_month'] = train_df['year'] * 100 + train_df['month']
    min_date = train_df['transaction_date'].min()
    train_df['months_since_min'] = ((train_df['transaction_date'].dt.year - min_date.year) * 12 + 
                                    train_df['transaction_date'].dt.month - min_date.month)
    
    # Property age features
    current_year = train_df['transaction_date'].dt.year
    train_df['property_age'] = current_year - train_df['lease_commence_year']
    train_df['property_age_squared'] = train_df['property_age'] ** 2
    train_df['property_age_log'] = np.log1p(train_df['property_age'])

    # Remaining lease features
    train_df['remaining_lease_years'] = train_df['remaining_lease_months'] / 12
    train_df['remaining_lease_log'] = np.log1p(train_df['remaining_lease_months'])
    train_df['remaining_lease_squared'] = train_df['remaining_lease_months'] ** 2
    train_df['remaining_lease_inverse'] = 1 / (train_df['remaining_lease_months'] + 1)

    # train_df['avg_storey'] = train_df['storey_range'].apply(extract_storey_numeric)
    # train_df['min_storey'] = train_df['storey_range'].apply(extract_storey_numeric)
    # train_df['max_storey'] = train_df['storey_range'].apply(extract_storey_numeric)

    # numeric_cols = ['floor_area_sqm', 'remaining_lease_months', 'avg_storey', 'min_storey', 'max_storey']
    # for col in numeric_cols:
    #     train_df[col] = train_df[col].fillna(train_df[col].median())

    # Remove storey_range
    numeric_cols = ['floor_area_sqm', 'remaining_lease_months']
    for col in numeric_cols:
        train_df[col] = train_df[col].fillna(train_df[col].median())

    # Additional features
    train_df['floor_area_squared'] = train_df['floor_area_sqm'] ** 2
    train_df['floor_area_cubed'] = train_df['floor_area_sqm'] ** 3
    train_df['floor_area_log'] = np.log1p(train_df['floor_area_sqm'])
    train_df['price_per_sqm'] = train_df['price'] / train_df['floor_area_sqm']
    train_df['lease_value_ratio'] = train_df['remaining_lease_months'] / (train_df['floor_area_sqm'] + 1)
    train_df['area_age_interaction'] = train_df['floor_area_sqm'] * train_df['property_age']
    #train_df['storey_area_interaction'] = train_df['avg_storey'] * train_df['floor_area_sqm']
    train_df['lease_age_interaction'] = train_df['remaining_lease_months'] * train_df['property_age']
    train_df['log_price'] = np.log1p(train_df['price'])

    # Feature groups
    # categorical_features = ['district', 'type', 'storey_range']
    # basic_numeric_features = ['floor_area_sqm', 'remaining_lease_months', 'avg_storey', 'min_storey', 'max_storey',
    #                         'year', 'month', 'quarter', 'property_age', 'months_since_min']
    # derived_numeric_features = ['floor_area_squared', 'floor_area_cubed', 'floor_area_log',
    #                             'property_age_squared', 'property_age_log', 'remaining_lease_log',
    #                             'remaining_lease_squared', 'remaining_lease_inverse', 'lease_value_ratio',
    #                             'area_age_interaction', 'storey_area_interaction', 'lease_age_interaction']
    # numeric_features = basic_numeric_features + derived_numeric_features
    # all_features = categorical_features + numeric_features

    categorical_features = ['district', 'type']
    basic_numeric_features = ['floor_area_sqm', 'remaining_lease_months',
                            'year', 'month', 'quarter', 'property_age', 'months_since_min']
    derived_numeric_features = ['floor_area_squared', 'floor_area_cubed', 'floor_area_log',
                                'property_age_squared', 'property_age_log', 'remaining_lease_log',
                                'remaining_lease_squared', 'remaining_lease_inverse', 'lease_value_ratio',
                                'area_age_interaction', 'lease_age_interaction']
    numeric_features = basic_numeric_features + derived_numeric_features
    all_features = categorical_features + numeric_features

    cutoff = '2021-01-01'
    train = train_df[train_df['transaction_date'] < cutoff].copy()
    val = train_df[train_df['transaction_date'] >= cutoff].copy()

    # Prepare X/y
    X_train = train[all_features]
    y_train = train['log_price']
    X_val = val[all_features]
    y_val = val['log_price']

    # Transformers
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

    # Models
    models = {
        'Linear': LinearRegression(),
        'Ridge': Ridge(alpha=1.0),
        'Lasso': Lasso(alpha=0.01),
        'ElasticNet': ElasticNet(alpha=0.01, l1_ratio=0.5)
    }


    X_train_fixed, y_train_fixed = check_and_fix_data(X_train, y_train)
    X_val_fixed, y_val_fixed = check_and_fix_data(X_val, y_val)

    # Train & evaluate
    for name, model in models.items():
        pipeline = Pipeline([
            ('preprocessor', preprocessor),
            ('model', model)
        ])
        pipeline.fit(X_train_fixed, y_train_fixed)
        
        y_pred_log = pipeline.predict(X_val_fixed)
        y_pred_log = np.maximum(y_pred_log, 0)
        
        y_val_orig = np.expm1(y_val_fixed)
        y_pred_orig = np.expm1(y_pred_log)
        
        rmse_log = np.sqrt(mean_squared_error(y_val_fixed, y_pred_log))
        rmse_orig = np.sqrt(mean_squared_error(y_val_orig, y_pred_orig))
        
        k = pipeline.named_steps['model'].coef_.shape[0]
        aic, bic = compute_aic_bic(y_val_fixed, y_pred_log, k)
        r2 = r2_score(y_val_fixed, y_pred_log)
        
        logging.info(f"{name} | R² (log): {r2:.3f} | RMSE (log): {rmse_log:.2f} | RMSE (original): {rmse_orig:,.2f} | AIC: {aic:.2f} | BIC: {bic:.2f}")

        test_df = pd.read_csv(test_file_path)
        test_df['transaction_date'] = pd.to_datetime(test_df['transaction_date'])

        # Repeat the same feature engineering steps
        test_df['year'] = test_df['transaction_date'].dt.year
        test_df['month'] = test_df['transaction_date'].dt.month
        test_df['quarter'] = test_df['transaction_date'].dt.quarter
        test_df['year_month'] = test_df['year'] * 100 + test_df['month']
        test_df['months_since_min'] = ((test_df['transaction_date'].dt.year - min_date.year) * 12 + 
                                        test_df['transaction_date'].dt.month - min_date.month)

        current_year_test = test_df['transaction_date'].dt.year
        test_df['property_age'] = current_year_test - test_df['lease_commence_year']
        test_df['property_age_squared'] = test_df['property_age'] ** 2
        test_df['property_age_log'] = np.log1p(test_df['property_age'])

        test_df['remaining_lease_years'] = test_df['remaining_lease_months'] / 12
        test_df['remaining_lease_log'] = np.log1p(test_df['remaining_lease_months'])
        test_df['remaining_lease_squared'] = test_df['remaining_lease_months'] ** 2
        test_df['remaining_lease_inverse'] = 1 / (test_df['remaining_lease_months'] + 1)

        # test_df['avg_storey'] = test_df['storey_range'].apply(extract_storey_numeric)
        # test_df['min_storey'] = test_df['storey_range'].apply(extract_storey_numeric)
        # test_df['max_storey'] = test_df['storey_range'].apply(extract_storey_numeric)

        for col in numeric_cols:
            test_df[col] = test_df[col].fillna(test_df[col].median())

        test_df['floor_area_squared'] = test_df['floor_area_sqm'] ** 2
        test_df['floor_area_cubed'] = test_df['floor_area_sqm'] ** 3
        test_df['floor_area_log'] = np.log1p(test_df['floor_area_sqm'])
        test_df['price_per_sqm'] = test_df['price'] / test_df['floor_area_sqm']
        test_df['lease_value_ratio'] = test_df['remaining_lease_months'] / (test_df['floor_area_sqm'] + 1)
        test_df['area_age_interaction'] = test_df['floor_area_sqm'] * test_df['property_age']
        #test_df['storey_area_interaction'] = test_df['avg_storey'] * test_df['floor_area_sqm']
        test_df['lease_age_interaction'] = test_df['remaining_lease_months'] * test_df['property_age']
        test_df['log_price'] = np.log1p(test_df['price'])

        X_test = test_df[all_features]
        y_test = test_df['log_price']
        y_test_orig = test_df['price']

        X_test_fixed, y_test_fixed = check_and_fix_data(X_test, y_test)

        y_test_log_pred = pipeline.predict(X_test_fixed)
        y_test_log_pred = np.maximum(y_test_log_pred, 0)
        y_test_pred = np.expm1(y_test_log_pred)

        rmse_test = np.sqrt(mean_squared_error(y_test_orig, y_test_pred))
        r2_test = r2_score(y_test_orig, y_test_pred)

        logging.info(f"{name} | TEST RMSE: {rmse_test:.2f} | TEST R²: {r2_test:.3f}")

        with PdfPages(pdf_path) as pdf:
            # 1. Add metrics page
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.axis('off')
            metrics_text = (
                f"Model: {name}\n\n"
                f"Test RMSE: {rmse_test:,.2f}\n"
                f"Test R²: {r2_test:.3f}\n"
            )
            ax.text(0, 1, metrics_text, verticalalignment='top', fontsize=12, family='monospace')
            pdf.savefig(fig)
            plt.close(fig)

            # 2. Plot actual vs predicted
            plt.figure(figsize=(8, 6))
            sns.scatterplot(x=y_test_orig, y=y_test_pred, alpha=0.4)
            plt.plot([y_test_orig.min(), y_test_orig.max()],
                     [y_test_orig.min(), y_test_orig.max()],
                     color='red', linestyle='--', lw=2)
            plt.xlabel("Actual Price")
            plt.ylabel("Predicted Price")
            plt.title(f"{name} - Actual vs Predicted Prices")
            plt.grid(True)
            pdf.savefig()
            plt.close()