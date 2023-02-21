####
# Injest water level CSVs created by s100_to_csv.py
# and return plots of residuals, RMSE and MAE
####

import os

import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme(style="whitegrid")

stn_stats_wlp = pd.DataFrame()
stn_stats_wlf = pd.DataFrame()
stn_residuals_wlp = pd.DataFrame()
stn_residuals_wlf = pd.DataFrame()

# Read CSVs in current directory
for i in os.listdir():
    if i.endswith('.csv'):
        data = pd.read_csv(i, index_col='datetime', na_values='-9999.000',parse_dates=True)
        # Generate residual data
        # wlo - wlp
        if 'wlp' in data.columns:
            data[i[:-4]] = data['wlo'] - data['wlp']
            stn_residuals_wlp= pd.concat([stn_residuals_wlp, data[i[:-4]]], axis=1)
            # wlo - wlp MAE
            mae = data[i[:-4]].abs().sum()/len(data[i[:-4]])
            # wlo - wlp RMSE
            rmse = (data[i[:-4]] ** 2).mean() ** .5
            stats = pd.DataFrame(data={'mae':mae, 'rmse':rmse},index=[i[:-4]])
            stn_stats_wlp = pd.concat([stn_stats_wlp, stats], axis=0)
      
        # wlo - wlf
        if 'wlf' in data.columns:
            data['wlo-wlf'] = data['wlo'] - data['wlf']
            stn_residuals_wlf= pd.concat([stn_residuals_wlf, data[i[:-4]]], axis=1)
            # wlo - wlp MAE
            mae = data[i[:-4]].abs().sum()/len(data[i[:-4]])
            # wlo - wlp RMSE
            rmse = (data[i[:-4]] ** 2).mean() ** .5
            stats = pd.DataFrame(data={'mae':mae, 'rmse':rmse},index=[i[:-4]])
            stn_stats_wlf = pd.concat([stn_stats_wlf, stats], axis=0)

stn_residuals_wlp = stn_residuals_wlp.dropna(axis=0, how='all')
stn_residuals_wlf = stn_residuals_wlf.dropna(axis=0, how='all')

# Generate Plots
# wlp Residuals
if not stn_residuals_wlp.empty:
    stn_residuals_wlp.plot(figsize=(15, 10),ylim =(-1,1))
    plt.title('WLO - WLP Residuals')
    plt.xticks(rotation=30, ha='right')
    plt.xlabel('Datetime')
    plt.ylabel('Residuals')
    plt.savefig('wlp_residuals.png')
    plt.clf()
# wlf Residuals
if not stn_residuals_wlf.empty:
    stn_residuals_wlf.plot(figsize=(15, 10),ylim =(-1,1))
    plt.title('WLO - WLF Residuals')
    plt.xticks(rotation=30, ha='right')
    plt.xlabel('Datetime')
    plt.ylabel('Residuals')
    plt.savefig('wlf_residuals.png')
    plt.clf()

if not stn_stats_wlp.empty:
    # wlp Errors
    stn_stats_wlp.plot.bar(figsize=(15, 10))
    plt.title('Errors - WLO/WLP')
    plt.xticks(rotation=30, ha='right')
    plt.xlabel('Stations')
    plt.ylabel('Errors')
    plt.savefig('wlp_MAE.png')
    plt.clf()
    # wlp RMSE
if not stn_stats_wlf.empty:
    # wlf Errors
    stn_stats_wlp.plot.bar(figsize=(15, 10))
    plt.title('Errors - WLO/WLF')
    plt.xticks(rotation=30, ha='right')
    plt.xlabel('Stations')
    plt.ylabel('Errors')
    plt.savefig('wlf_MAE.png')
    plt.clf()