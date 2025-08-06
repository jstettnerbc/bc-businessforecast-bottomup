import os
import sys
import yaml
sys.path.append(os.path.dirname(os.getcwd()))
from sqlalchemy import create_engine
import datetime
import tqdm
import warnings
import clickhouse_driver
from connection_configs import ClickhouseConnectionConfig
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

warnings.filterwarnings('ignore')

# %%

dwh_db_cfg = ClickhouseConnectionConfig().export_connection_dict()

# %%

### OVERWRITE THE host and user and password in the .env before
dwh_db_conn2 = clickhouse_driver.dbapi.connect(
                        host=dwh_db_cfg["host"],
                        port=9440,#datalake_db_cfg["port"],
                        database='analytics', 
                        #dwh_db_cfg["database"],
                        user=dwh_db_cfg["user"],
                        password=dwh_db_cfg["password"],
                        secure=True
                        )


# %%
available_query = """

select itemNo, 
toStartOfMonth(ts.snapshotDate) as dd, 
count(*) as daysWithSnapshot,
sum(ts.webshop_isListedOnline) as daysListedOnline ,
sum(ts.webshop_orderableOrAvailable) as daysAvailableOnline
from
reporting.timeseries_stock ts 
left join analytics.item_dimensions id on id.itemNo = ts.itemNo
where  ts.snapshotDate >= '2023-07-01' and ts.snapshotDate  < '2025-08-01' 
and id.webshop_isListedOnline = True

group by itemNo, toStartOfMonth(ts.snapshotDate)
order by toStartOfMonth(ts.snapshotDate) desc
"""

available_df = pd.read_sql(available_query, dwh_db_conn2)

sales_query = """

select itemNo, 
toStartOfMonth(is2.postingDate) as pd, 
sum(salesAmountActual) as salesAmountPerMonth ,
sum(quantity) as salesQuantityPerMonth 
from
analytics.item_salesanalytics is2
left join analytics.item_dimensions id on id.itemNo = is2.itemNo
where  is2.postingDate >= '2023-07-01' and is2.postingDate < '2025-08-01' 
and  id.webshop_isListedOnline = True
group by itemNo, toStartOfMonth(is2.postingDate)
"""

sales_df = pd.read_sql(sales_query, dwh_db_conn2)

# %%
combined_df = available_df.merge(sales_df, how="left", left_on=["itemNo", "dd"], right_on=["itemNo", "pd"])
combined_df.salesAmountPerMonth = combined_df.salesAmountPerMonth.fillna(0.)
combined_df.salesQuantityPerMonth = combined_df.salesQuantityPerMonth.fillna(0.)
combined_df["meanAvailability"] = combined_df.daysAvailableOnline/combined_df.daysListedOnline
combined_df.drop(columns="pd", inplace=True)

def fitSinusiod(df):

    # Ensure 'dd' is datetime and sorted
    df['dd'] = pd.to_datetime(df['dd'])
    df = df.sort_values('dd').reset_index(drop=True)

    # Encode time as months since first observation
    df['month_num'] = (df['dd'].dt.year - df['dd'].dt.year.min()) * 12 + (df['dd'].dt.month - df['dd'].dt.month.min())

    # Select data where meanAvailability > 0.2 (for fitting)
    mask = df['meanAvailability'] > 0.2
    
    if mask.mean() < 0.2:
        # Too much good availability: skip fitting and return NULL (NaN)
        df['sinusoidal_prediction'] = np.nan
        # print(f"Skipping sinusoidal fit because availability was too bad")
    else:
        # Proceed with sinusoidal fit

        x_fit = df.loc[mask, 'month_num'].values.reshape(-1, 1)
        y_fit = df.loc[mask, 'salesQuantityPerMonth'].values

        # Frequency for yearly seasonality: 1 cycle per 12 months
        freq = 1 / 12

        # Prepare design matrix for linear regression with sine and cosine terms + intercept
        X_fit = np.column_stack([
            np.sin(2 * np.pi * freq * x_fit.flatten()),
            np.cos(2 * np.pi * freq * x_fit.flatten()),
        ])

        # Create and train linear regression model (fit_intercept=True adds offset)
        model = LinearRegression()
        model.fit(X_fit, y_fit)

        A_sin, A_cos = model.coef_
        offset = model.intercept_

        # Calculate amplitude and phase
        amplitude = np.sqrt(A_sin**2 + A_cos**2)
        phase = np.arctan2(A_cos, A_sin)

        # print(f"Fitted parameters:\n Amplitude: {amplitude:.2f}\n Phase: {phase:.2f} radians\n Offset: {offset:.2f}")

        # Define function to calculate fitted sinusoidal value
        def sinusoidal_fit(t):
            return amplitude * np.sin(2 * np.pi * freq * t + phase) + offset

        # Apply fitted function to all months
        df['sinusoidal_prediction'] = sinusoidal_fit(df['month_num'])
        
        # Clip negative values to zero in the sinusoidal prediction
        df['sinusoidal_prediction'] = df['sinusoidal_prediction'].clip(lower=0)

    return df['sinusoidal_prediction'].values


def correctForMissedSales(df):
    # Dreisatz with fallback to NaN
    import numpy as np
    df['correctedDreisatz'] = np.where(
        df['daysAvailableOnline'] < 1,
        np.nan,
        df['salesQuantityPerMonth'] * df['daysListedOnline'] / df['daysAvailableOnline']
    )

    
    # meanNeighbors
    prev_neighbor = df['salesQuantityPerMonth'].shift(1)
    next_neighbor = df['salesQuantityPerMonth'].shift(-1)
    df['correctedMeanNeighbors'] = (prev_neighbor.fillna(0) + next_neighbor.fillna(0)) / (prev_neighbor.notna().astype(int) + next_neighbor.notna().astype(int))

    # Fit a sine curve and fill
    df["correctedSinusSeasonal"] = fitSinusiod(df)

    # Filter out zeros and NaNs
    valid_sales = df['salesQuantityPerMonth'][(df['salesQuantityPerMonth'] != 0) & (df['salesQuantityPerMonth'].notna())]

    # Compute cap_value as twice the mean of these valid sales values
    cap_value = 2. * valid_sales.mean()
    
    # Stack the two correction columns horizontally (shape: n_rows x 2)
    corrections = np.vstack([df['correctedDreisatz'], df['correctedSinusSeasonal']]).T

    # Compute mean ignoring NaNs row-wise
    mean_correction = np.nanmean(corrections, axis=1)

    # If a row has all NaNs, np.nanmean returns nan by default, 
    # which is okay. You can fill those later if you want.

    # Apply the capped correction where availability <= 0.8, else use original salesQuantityPerMonth
    df['correctedCombined'] = np.where(
        df['meanAvailability'] > 0.8,
        df['salesQuantityPerMonth'],
        np.minimum(mean_correction, cap_value)
    )

    return df
    

import_correctSalesToDiskover = []
for testItemNo in tqdm.tqdm(combined_df.itemNo.values):#['1000004203','1000143783']):#,'1000314922','1000143783','1000000401','1000004203','1000065221','1000253999','1000226431']):
    # get some info about the test_itemNo
    test_df = combined_df.loc[combined_df.itemNo==testItemNo]
    test_df = correctForMissedSales(test_df)

    # Get max date in 'dd'
    max_date = test_df['dd'].max()
    # Calculate cutoff date exactly 12 months before max_date
    # Note: subtracting 1 year using DateOffset since .dt.year and .dt.month are int
    cutoff_date = max_date - pd.DateOffset(months=12)

    # Filter for last twelve months (strictly greater than cutoff_date)
    ltm_df = test_df.loc[test_df['dd'] > cutoff_date]

    ltm_df = ltm_df[["itemNo", "dd", "correctedCombined"]].rename(
        columns={"itemNo" : "Materialnummer", "dd" : "Periode", "correctedCombined" : "Menge"})
    ltm_df["Werk"] = "BC"
    ltm_df["Kommentar"] = ""
    import_correctSalesToDiskover.append(ltm_df[['Werk', 'Materialnummer', 'Periode', 'Menge', 'Kommentar']])


import_correctSalesToDiskover = pd.concat(import_correctSalesToDiskover, ignore_index=True)
timestamp = datetime.datetime.now().strftime("%Y-%m-%d:%H-%M")

import_correctSalesToDiskover.to_csv("./data/"+timestamp+"_missedSalesCorrectionForDiskover.csv", index=False, sep=";")

# %%


