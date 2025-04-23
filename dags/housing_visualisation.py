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
    df = pd.read_csv(input_csv_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    df = df.dropna(subset=['transaction_date'])

    # 1. Monthly Trend of Average Housing Prices
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

    # 2. Storey Range : HDB vs URA
    for type in ['HDB', 'URA']:
        subset = df[df['type'].str.contains(ttype, na=False)]
        if subset.empty:
            continue
        plt.figure(figsize=(10, 6))
        sns.boxplot(data=subset, x='storey_range', y='price_per_sqm')
        plt.title(f"Price per SQM by Storey Range ({ttype})")
        plt.xticks(rotation=45)
        plt.tight_layout()
        filename = f'floor_level_vs_price_{ttype.lower()}.png'
        plt.savefig(os.path.join(output_dir, filename))
        plt.clf()

    # 3. Top 5/lower 5
    avg_price_by_town = df.groupby('district')['price_per_sqm'].mean().sort_values()
    top5 = avg_price_by_town[-5:]
    bottom5 = avg_price_by_town[:5]
    selected = pd.concat([bottom5, top5])

    plt.figure(figsize=(10, 6))
    sns.barplot(x=selected.values, y=selected.index)
    plt.title("Top/Bottom 5 Avg Price per SQM by Town")
    plt.xlabel("SGD per SQM")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'top_bottom5_town_avg_price.png'))
    plt.clf()

    # 4. Correlation Heatmap
    corr_cols = ['price', 'interest_rate', 'cpi', 'unemployment_rate']
    plt.figure(figsize=(8, 6))
    sns.heatmap(df[corr_cols].corr(), annot=True, cmap='coolwarm')
    plt.title("Correlation Heatmap")
    plt.savefig(os.path.join(output_dir, 'heatmap_corr.png'))
    plt.clf()

    # 5. Lease Duration vs Price 
    lease_filtered = df[df['remaining_lease_months'] <= 1200]  
    plt.figure(figsize=(8, 6))
    sns.scatterplot(data=lease_filtered, x='remaining_lease_months', y='price_per_sqm', alpha=0.5)
    plt.title("Remaining Lease (<=999y) vs Price per SQM")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'lease_vs_price_filtered.png'))
    plt.clf()

    # 6. Boxplot by Property Type
    plt.figure(figsize=(8, 6))
    sns.boxplot(data=df, x='type', y='price_per_sqm')
    plt.title("Price per SQM by Property Type")
    plt.xticks(rotation=45)
    plt.tight_layout()
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
