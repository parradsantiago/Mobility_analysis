# ==============================================================================
# PROJECT: URBAN MOBILITY & ECONOMIC INDICATORS ANALYSIS (OECD & TOMTOM 2024)
# Author: Santiago Parra (Data Analytics Portfolio)
# Description: This script cleans, merges, and visualizes traffic congestion 
#              data against city-level economic KPIs.
# ==============================================================================

# 1. LIBRARY IMPORT
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Visualization Global Settings
sns.set_theme(style="whitegrid")

# 2. DATA ACQUISITION
# Note: Use relative paths when uploading to GitHub for better portability
path_traffic = r'C:\Users\U1080701\OneDrive - Sanofi\Documentos\Python projects\tomtom_traffic.csv'
path_eco = r'C:\Users\U1080701\OneDrive - Sanofi\Documentos\Python projects\oecd_city_economy.csv'

traffic = pd.read_csv(path_traffic)
eco = pd.read_csv(path_eco)

# 3. SCHEMA STANDARDIZATION (SNAKE_CASE)
traffic = traffic.rename(columns={
    'City': 'city', 'Country': 'country', 'UpdateTimeUTC': 'update_time_utc',
    'JamsDelay': 'jams_delay', 'TrafficIndexLive': 'traffic_index_live',
    'JamsLengthInKms': 'jams_length_kms', 'JamsCount': 'jams_count',
    'TrafficIndexWeekAgo': 'traffic_index_week_ago', 
    'UpdateTimeUTCWeekAgo': 'update_time_utc_week_ago',
    'TravelTimeLivePer10KmsMins': 'travel_time_live_per_10kms_mins',
    'TravelTimeHistoricPer10KmsMins': 'travel_time_hist_per_10kms_mins',
    'MinsDelay': 'mins_delay'
})

eco = eco.rename(columns={
    'Country': 'country', 'Year': 'year', 'City': 'city',
    'City GDP/capita': 'city_gdp_capita', 'Unemployment %': 'unemployment_pct',
    'PM2.5 (μg/m³)': 'pm25_concentration', 'Population (M)': 'population_m'
})

# 4. TEMPORAL DATA PROCESSING
traffic['update_time_utc'] = pd.to_datetime(traffic['update_time_utc'], errors='coerce')
traffic['update_time_utc_week_ago'] = pd.to_datetime(traffic['update_time_utc_week_ago'], errors='coerce')
traffic['year'] = traffic['update_time_utc'].dt.year

# 5. DATA WRANGLING: STRING CLEANING & NUMERIC CONVERSION
# Removing thousand separators (.) and aligning decimal separators (,) to (.)
eco['city_gdp_capita'] = eco['city_gdp_capita'].astype(str).str.strip().str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
eco['unemployment_pct'] = eco['unemployment_pct'].astype(str).str.strip().str.replace('%', '', regex=False).str.replace(',', '.', regex=False)
eco['population_m'] = eco['population_m'].astype(str).str.strip().str.replace(',', '.', regex=False)

# Batch conversion to numeric types
cols_num = ['city_gdp_capita', 'unemployment_pct', 'population_m']
for col in cols_num:
    eco[col] = pd.to_numeric(eco[col], errors='coerce')

# Feature Engineering: Absolute Population
eco['population'] = eco['population_m'] * 1_000_000

# 6. FILTERING & AGGREGATIONS (2024 FOCUS)
traffic_2024 = traffic[traffic['year'] == 2024].copy()
eco_2024 = eco[eco['year'] == 2024].copy()

# Calculate mean traffic metrics per city
traffic_city_avg = traffic_2024.groupby(['city', 'year', 'country'])[[
    'jams_delay', 'traffic_index_live', 'jams_length_kms', 'jams_count', 
    'mins_delay', 'travel_time_live_per_10kms_mins', 'travel_time_hist_per_10kms_mins'
]].mean().add_suffix('_mean').reset_index()

# 7. DATA MERGING (UNIFYING MOBILITY & ECONOMY)
left_cols = [
    'city', 'country', 'year', 'jams_delay_mean', 'traffic_index_live_mean',
    'jams_length_kms_mean', 'jams_count_mean', 'mins_delay_mean',
    'travel_time_live_per_10kms_mins_mean', 'travel_time_hist_per_10kms_mins_mean'
]
right_cols = ['city', 'year', 'city_gdp_capita', 'unemployment_pct', 'pm25_concentration', 'population']

merged = pd.merge(traffic_city_avg[left_cols], eco_2024[right_cols], on=['city', 'year'], how='inner')

# 8. DATA VISUALIZATION
# 8.1 Traffic Delay Distribution (Boxplot)
plt.figure(figsize=(10, 6))
mean_delay = merged['jams_delay_mean'].mean()
sns.boxplot(y=merged['jams_delay_mean'], color='skyblue')
plt.axhline(mean_delay, color='purple', linestyle='--', label=f'Global Average: {mean_delay:.2f}')
plt.title('Traffic Delay Distribution by City (2024)')
plt.ylabel('Average Delay (Minutes)')
plt.legend()
plt.show()

# 8.2 GDP per Capita Distribution (Histogram)
plt.figure(figsize=(10, 5))
sns.histplot(merged['city_gdp_capita'], kde=True, color='pink', bins=20)
plt.axvline(merged['city_gdp_capita'].median(), color='orange', linestyle='--', label='Median GDP')
plt.title('City GDP per Capita Distribution (USD) - 2024')
plt.xlabel('GDP per Capita')
plt.legend()
plt.show()

# 8.3 Comparative Analysis: Congestion vs. Economy
# Using secondary Y-axis to handle different scales (Minutes vs. USD)
ax = merged.sort_values('jams_delay_mean', ascending=False).plot(
    kind='bar', x='city', y=['jams_delay_mean', 'city_gdp_capita'], 
    figsize=(15, 7), secondary_y='city_gdp_capita'
)
plt.title('Correlation Analysis: Traffic Congestion vs. GDP per Capita')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 9. RESULTS EXPORT
merged.to_csv("mobility_economy_analysis_2024.csv", index=False)
print("Analysis complete. Dataset exported successfully.")