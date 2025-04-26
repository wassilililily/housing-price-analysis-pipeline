from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import os

def classify_type(t):
        if t == 'HDB':
            return 'HDB'
        elif t == 'Executive':
            return 'Executive'
        else:
            return 'Private'
        
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
    monthly_avg_hdb = df_monthly[df_monthly['type'] == 'HDB']['price_per_sqm'].resample('M').mean()


    plt.figure(figsize=(10, 6))
    monthly_avg.plot(label='Overall', color='steelblue', linewidth=2)
    monthly_avg_hdb.plot(label='HDB', color='orange', linestyle='--', linewidth=2)
    plt.title("Monthly Average Price per SQM")
    plt.ylabel("SGD per SQM")
    plt.xlabel("Month")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'monthly_avg_price_trend.png'))
    plt.clf()

    # 2. Correlation Heatmap
    corr_cols = ['price','exchange_rate','interest_rate','cpi','unemployment_rate','median_household_income','floor_area_sqm',
        'storey_range','remaining_lease_months']

    df.set_index('transaction_date', inplace=True)
    df_numeric = df.select_dtypes(include=[np.number])
    monthly_df = df_numeric.resample('M').mean()
    plt.figure(figsize=(18, 14))
    sns.heatmap(monthly_df[corr_cols].corr(), annot=True, cmap='coolwarm')
    plt.title("Correlation Heatmap")
    plt.savefig(os.path.join(output_dir, 'heatmap_corr.png'))
    plt.clf()

    # 3. Top 5/Least 5
    avg_price_by_town = df.groupby('district')['price_per_sqm'].mean().sort_values()
    top5 = avg_price_by_town[-5:]
    bottom5 = avg_price_by_town[:5]
    selected = pd.concat([bottom5, top5])

    plt.figure(figsize=(10, 6))
    sns.barplot(x=selected.values, y=selected.index)
    plt.title("Top/Least 5 Avg Price per SQM by Town")
    plt.xlabel("SGD per SQM")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'top_least5_town_avg_price.png'))
    plt.clf()

    # 4. Lease Duration vs Price 
    lease_filtered = df[df['remaining_lease_months'] <= 1200]
    plt.figure(figsize=(8, 6))
    sns.regplot(
        data=lease_filtered,
        x='remaining_lease_months',
        y='price_per_sqm',
        scatter_kws={'alpha': 0.5}, 
        line_kws={'color': 'red'},   
        ci=95                      
    )
    plt.title("Remaining Lease (<=99 years) vs Price per SQM")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'lease_vs_price_filtered.png'))
    plt.clf()

    # 5. Boxplot by Property Type
    plt.figure(figsize=(8, 6))
    sns.boxplot(data=df, x='type', y='price_per_sqm')
    plt.title("Price per SQM by Property Type")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'type_vs_price.png'))
    plt.clf()

    # 6. Storey Range : HDB vs Executive vs Private
    df['type_group'] = df['type'].apply(classify_type)
    fig, axes = plt.subplots(1, 3, figsize=(20, 6), sharey=True)
    group_order = ['HDB', 'Executive', 'Private']
    for ax, type_group in zip(axes, group_order):
        subset = df[df['type_group'] == type_group]
        if not subset.empty:
            sns.boxplot(data=subset.reset_index(drop=True), x='storey_range', y='price_per_sqm', ax=ax)
            ax.set_title(f"{type_group}")
            ax.set_xlabel("Storey Range")
            ax.set_ylabel("Price per SQM" if type_group == 'HDB' else "")
            ax.tick_params(axis='x', rotation=45)

    # Adjust layout and save
    plt.suptitle("Price per SQM by Storey Range for HDB, Executive, and Private Housing", fontsize=14)
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    filename = os.path.join(output_dir, 'floor_level_vs_Price_three_types.png')
    plt.savefig(filename)
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
