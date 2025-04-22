from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

# Visualization Task
@task
def generate_visualisations(input_csv_path: str, output_dir: str):
    # Load data
    df = pd.read_csv(input_csv_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # 1. Monthly Trend of Average Housing Prices
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    df = df.dropna(subset=['transaction_date'])

    df_monthly = df.set_index('transaction_date').sort_index()
    monthly_avg = df_monthly['price_per_sqm'].resample('M').mean()

    plt.figure(figsize=(10, 6))
    monthly_avg.plot()
    plt.title("Monthly Average Price per SQM")
    plt.ylabel("SGD per SQM")
    plt.xlabel("Month")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'monthly_avg_price_trend.png'))
    plt.clf()

    # 2. Price Distribution Across Floor Level Categories
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x='storey_range', y='price_per_sqm')
    plt.title("Price per SQM by Storey Range")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'floor_level_vs_price.png'))
    plt.clf()

    # 3. Average Price per Town/Region (Bar)
    plt.figure(figsize=(12, 6))
    avg_price_by_town = df.groupby('district')['price_per_sqm'].mean().sort_values(ascending=False)
    sns.barplot(x=avg_price_by_town.values, y=avg_price_by_town.index)
    plt.title("Average Price per SQM by Town")
    plt.xlabel("SGD per SQM")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'town_avg_price.png'))
    plt.clf()

    # 4. Correlation Heatmap
    corr_cols = ['price','interest_rate', 'cpi', 'unemployment_rate']
    plt.figure(figsize=(8, 6))
    sns.heatmap(df[corr_cols].corr(), annot=True, cmap='coolwarm')
    plt.title("Correlation Heatmap")
    plt.savefig(os.path.join(output_dir, 'heatmap_corr.png'))
    plt.clf()

    # 5. Lease Duration vs Price
    plt.figure(figsize=(8, 6))
    sns.scatterplot(data=df, x='remaining_lease_months', y='price_per_sqm', alpha=0.5)
    plt.title("Remaining Lease vs Price per SQM")
    plt.savefig(os.path.join(output_dir, 'lease_vs_price.png'))
    plt.clf()

    # 6. Boxplot by Property Type
    plt.figure(figsize=(8, 6))
    sns.boxplot(data=df, x='type', y='price_per_sqm')
    plt.title("Price per SQM by Property Type")
    plt.xticks(rotation=45)
    plt.savefig(os.path.join(output_dir, 'type_vs_price.png'))
    plt.clf()

# DAG Definition
with DAG(
    dag_id='generate_visualisations_dag',
    start_date=days_ago(1),
    schedule_interval=None,  
    catchup=False,
    tags=['visualization', 'housing', 'etl']
) as dag:

    visualize_task = generate_visualisations(
        input_csv_path='/opt/airflow/data/housing_data.csv',     
        output_dir='/opt/airflow/data/graph'                
    )

    visualize_task
