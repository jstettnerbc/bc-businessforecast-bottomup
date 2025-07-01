#!/usr/bin/env python
# -*- coding: utf-8 -*-

import warnings
import os
from datetime import datetime
import numpy as np
import pandas as pd
import yaml
from sqlalchemy import create_engine
import tqdm
import clickhouse_driver
from clickhouse_driver import Client
import concurrent.futures
import typer

# Hardcoded path for manual forecast Excel file
MANUALFORECAST_FILE = "data/20250701_manual_demand_estimates.xlsx"

# Read the Excel file once at module load
if os.path.exists(MANUALFORECAST_FILE):
    manual_forecast_df = pd.read_excel(MANUALFORECAST_FILE)
else:
    manual_forecast_df = None

from connection_configs import ClickhouseConnectionConfig
warnings.filterwarnings('ignore')

app = typer.Typer()

dwh_db_cfg = ClickhouseConnectionConfig().export_connection_dict()
dwh_db_conn2 = clickhouse_driver.dbapi.connect(
    host=dwh_db_cfg["host"],
    port=9440,
    database='analytics',
    user=dwh_db_cfg["user"],
    password=dwh_db_cfg["password"],
    secure=True
)

def get_demand_fc(item_no, scenario_config=None):
    query = f"""

select month_startdate, forecasted_demand_adjusted, forecasted_demand_statistical, brand
              from analytics.diskover_demand_forecast fc
              left join analytics.item_dimensions id on id.itemNo = toString(fc.bc_item_no)
              where bc_item_no = '{item_no}' and month_startdate >= toStartOfMonth(today()) 
              order by month_startdate """
    fc = pd.read_sql(query, dwh_db_conn2)

    if fc.empty:
        # Create a date range for 12 months starting from current month
        start_date = pd.Timestamp(datetime.today().strftime('%Y-%m-01'))
        dates = pd.date_range(start=start_date, periods=12, freq='MS')  # 'MS' = Month Start

        if manual_forecast_df is not None:
            # print("Fallbacking to manual forecast for item:", item_no)
            manual_demand = manual_forecast_df[manual_forecast_df['itemNo'] == item_no]
            if not manual_demand.empty:
                manual_demand = manual_demand.manual_12m_estimate.values[0]
                fc = pd.DataFrame({
                    'month_startdate' : dates,
                    'forecasted_demand_adjusted' : manual_demand/12.
                })
            else:
                # Create new empty forecast with zero demand
                fc = pd.DataFrame()

    if fc.empty:
            # Create new empty forecast
            # print("Last fallback, using zero. No demand forecast found for item:", item_no)
            fc = pd.DataFrame({
                'month_startdate': dates,
                'forecasted_demand_adjusted': 0
            })

    # Convert month_startdate to datetime and set as index
    fc['month_startdate'] = pd.to_datetime(fc['month_startdate'])
    fc.set_index('month_startdate', inplace=True)
    
    # Apply uplift if scenario_config is provided and brand matches
    if scenario_config is not None and 'brand' in scenario_config and 'uplift_factor' in scenario_config:
        # Get the brand for this item (from the forecast DataFrame)
        if not fc.empty and 'brand' in fc.columns:
            item_brand = fc['brand'].iloc[0]
            if item_brand == scenario_config['brand']:
                fc['forecasted_demand_adjusted'] = fc['forecasted_demand_adjusted'] * (1 + float(scenario_config['uplift_factor']))
    
    # Create a date range with weekly frequency covering the full span of data
    weekly_index = pd.date_range(start=fc.index.min(), end=fc.index.max() + pd.offsets.MonthEnd(0), freq='W-SUN')
    
    # Reindex the DataFrame to this weekly frequency
    # We'll forward-fill the monthly demand to all weeks but adjust demand by dividing monthly demand by number of weeks within that month
    def distribute_monthly_to_weeks(df, weekly_idx):
        # Create an empty DataFrame with weekly index
        weekly_df = pd.DataFrame(index=weekly_idx)
        # For each month, find the weeks that fall within that month
        for month_start, row in df.iterrows():
            month_end = month_start + pd.offsets.MonthEnd(0)
            # Weeks within this month
            weeks_in_month = weekly_idx[(weekly_idx >= month_start) & (weekly_idx <= month_end)]
            # Divide monthly demand evenly among weeks
            weekly_demand = row['forecasted_demand_adjusted'] / len(weeks_in_month) if len(weeks_in_month) > 0 else 0
            # Assign the divided demand to those weeks
            weekly_df.loc[weeks_in_month, 'forecasted_demand_adjusted'] = weekly_demand
        # Fill any missing demand with 0 (if any weeks don't belong to any month)
        weekly_df['forecasted_demand_adjusted'].fillna(0, inplace=True)
        return weekly_df
    
    weekly_forecast = distribute_monthly_to_weeks(fc, weekly_index)
    weekly_forecast.reset_index(inplace=True)
    weekly_forecast.rename(columns={'index': 'week_ending_date'}, inplace=True)

    # Filter: Only keep weeks >= this week's Sunday
    today = pd.Timestamp(datetime.today())
    this_week_end = today + pd.offsets.Week(weekday=6)
    weekly_forecast = weekly_forecast[weekly_forecast['week_ending_date'] >= this_week_end.normalize()].reset_index(drop=True)

    return weekly_forecast


sammel_PO_ids = [
    "BE100600", "BE100624", "BE100631", "BE100632", "BE100633", "BE100634",
    "BE100713", "BE100714", "BE100715", "BE100716", "BE100718", "BE100719",
    "BE100723", "BE100724", "BE100725", "BE100726", "BE100727", "BE100730",
    "BE100733", "BE100735", "BE100738", "BE100739", "BE100741", "BE100743", 
    "BE100744", "BE100745", "BE100746", "BE100747", "BE100748", "BE100749", 
    "BE100750", "BE100751", "BE100752", "BE100754", "BE100756", "BE100758", 
    "BE100759", "BE100760", "BE100763", "BE100828", "BE100835", "BE100836", 
    "BE100839", "BE100840", "BE100841", "BE100842", "BE100843", "BE100844", 
    "BE100845", "BE100846", "BE100849", "BE100945", "BE100947", "BE100948", 
    "BE100949", "BE100953", "BE101086", "BE101309", "BE101313", "BE102004"
]


def get_incoming_pos(item_no, filterout_sammelpos=False):
    query = f"""
        select documentNo, orderStatus, itemNo, quantityOrdered-quantityReceived as quantityOpen, unitCostThisPurchaseLine, expectedReceiptDate
        from reporting.open_purchaseorder_lines where itemNo = '{item_no}' and expectedReceiptDate < '2030-01-01' order by expectedReceiptDate"""
    
    inc = pd.read_sql(query, dwh_db_conn2)
    
    if inc.empty:
        # Create a weekly date range for 16 months (~69 weeks) starting from current week ending Sunday
        start_date = pd.Timestamp(datetime.today())
        start_week_end = start_date + pd.offsets.Week(weekday=6)  # Sunday
        periods = 53  # about 24 months in weeks
        dates = pd.date_range(start=start_week_end.normalize(), periods=periods, freq='W-SUN')
        inc_resampled = pd.DataFrame({
            'week_ending_date': dates,
            'quantityOpen': 0,
            'unitCostThisPurchaseLine': 0
        })
    else:
        if filterout_sammelpos:
            inc = inc[~inc['documentNo'].isin(sammel_PO_ids)]
        
        inc['expectedReceiptDate'] = pd.to_datetime(inc['expectedReceiptDate']).dt.normalize()
        inc['week_ending_date'] = inc['expectedReceiptDate'] + pd.offsets.Week(weekday=6)
        inc['week_ending_date'] = inc['week_ending_date'].dt.normalize()  # remove time part and milliseconds
        
        inc_resampled = inc.groupby('week_ending_date').agg({
            'quantityOpen': 'sum',
            'unitCostThisPurchaseLine': 'mean'
        })
        
        start_date = pd.Timestamp(datetime.today())
        start_week_end = (start_date + pd.offsets.Week(weekday=6)).normalize()
        periods = 53  # 24 months approx in weeks
        full_range = pd.date_range(start=start_week_end, periods=periods, freq='W-SUN')
        
        overdue_quantity = inc_resampled.loc[inc_resampled.index < start_week_end, 'quantityOpen'].sum()
        overdue_price = inc_resampled.loc[inc_resampled.index < start_week_end, 'unitCostThisPurchaseLine'].mean()
        
        inc_resampled = inc_resampled.loc[inc_resampled.index >= start_week_end]
        
        inc_resampled = inc_resampled.reindex(full_range, fill_value=0)
        
        # For unitCostThisPurchaseLine, fill with NaN or with previous non-null value if any
        if inc_resampled['unitCostThisPurchaseLine'].isna().all():
            inc_resampled['unitCostThisPurchaseLine'] = 0
        else:
            inc_resampled['unitCostThisPurchaseLine'].fillna(method='ffill', inplace=True)
            inc_resampled['unitCostThisPurchaseLine'].fillna(0, inplace=True)
        
        inc_resampled.iloc[0, inc_resampled.columns.get_loc('quantityOpen')] += overdue_quantity
        inc_resampled.iloc[0, inc_resampled.columns.get_loc('unitCostThisPurchaseLine')] = overdue_price if not np.isnan(overdue_price) else 0
        
        inc_resampled = inc_resampled.reset_index().rename(columns={'index': 'week_ending_date'})
        # print(inc_resampled.head(6))
    
    # Filter: Only keep weeks >= this week's Sunday
    this_week_end = pd.Timestamp(datetime.today()) + pd.offsets.Week(weekday=6)
    inc_resampled = inc_resampled[inc_resampled['week_ending_date'] >= this_week_end.normalize()].reset_index(drop=True)
    return inc_resampled
        
def get_item_facts(item_no):
    query = f"""
        with meanSalesprice as (select itemNo, avg(is2.salesAmountActual/is2.quantity) as mean_salesprice from analytics.item_salesanalytics is2
where is2.postingDate > '2024-01-01' and is2.quantity > 0 and is2.salesAmountActual> 0
group by is2.itemNo)


select 
        id.itemNo, id.masterItemNo, id.itemDescription, id.itemDescription2, id.brand,  
        if2.availableStock, if2.salesPriceBruttoDE, 
        if2.unitCost,  if2.salesPiecesLast360d, id.abcClass, id.xyzClass
        ,
        id.brand as brand,id.brandSector as brandSector, id.wareGroupLevel0 as wareGroupLevel0, id.wareGroupLevel1 as wareGroupLevel1,
id.wareGroupLevel2 as wareGroupLevel2, id.itemStatusCode as itemStatusCode,
id.abcClass as abcClass, id.xyzClass as xyzClass,
id.diskoverControllingGroup as diskoverControllingGroup,
id.webshop_isListedOnline as webshop_isListedOnline,
id.webshop_orderableOrAvailable as webshop_orderableOrAvailable,
id.*,
(if2.salesPriceBruttoDE * 100.) / (100 + toInt32(eie.vatProdPostingGroup)) AS salesPriceNettoDE,
msp.mean_salesprice

        from analytics.item_dimensions id 
        left join analytics.item_facts if2 on if2.itemNo  = id.itemNo 
        LEFT JOIN analytics.erp_item_export AS eie ON eie.itemNo = id.itemNo
        left join meanSalesprice msp on msp.itemNo = id.itemNo
        where id.itemNo = '{item_no}'  """
    facts = pd.read_sql(query, dwh_db_conn2)

    return facts


def build_full_forecast(available_stock,
                        current_unitcost, 
                        current_netprice, 
                        current_status,
                        incoming_purchases_df,
                        outgoing_demand_df):
    
    incoming_summary = incoming_purchases_df.set_index('week_ending_date').rename(columns={'quantityOpen': 'incoming_qty', 'unitCostThisPurchaseLine': 'avg_unit_cost'}).copy()

    outgoing_qty = outgoing_demand_df.set_index('week_ending_date')[['forecasted_demand_adjusted']].rename(columns={'forecasted_demand_adjusted': 'planned_outgoing_qty'}).copy()

    # Combine all months covering incoming and outgoing
    stock_df = pd.merge(incoming_summary, outgoing_qty, left_index=True, right_index=True, how='outer').fillna(0)
    stock_df = stock_df.sort_index().reset_index()

    # Filter: Only keep weeks >= this week's Sunday
    this_week_end = pd.Timestamp(datetime.today()) + pd.offsets.Week(weekday=6)
    stock_df = stock_df[stock_df['week_ending_date'] >= this_week_end.normalize()].reset_index(drop=True)

    stock_balance = available_stock
    stock_balances = []
    lost_sales_qty = []
    fulfilled_demands = []

    for idx, row in stock_df.iterrows():
        stock_balance += row['incoming_qty']

        # Fulfill demand only up to available stock
        fulfilled_demand = min(row['planned_outgoing_qty'], stock_balance)
        stock_balance -= fulfilled_demand

        # Lost sales = demand not fulfilled due to insufficient stock
        lost_qty = row['planned_outgoing_qty'] - fulfilled_demand

        #print(f"Week ending {row['week_ending_date']}: ")
        #print(f"  Incoming: {row['incoming_qty']}, Planned outgoing: {row['planned_outgoing_qty']}, ")
        #print(f"  Stock balance after: {stock_balance}, Fulfilled demand: {fulfilled_demand}, Lost sales: {lost_qty}")

        lost_sales_qty.append(lost_qty)
        stock_balances.append(stock_balance)
        fulfilled_demands.append(fulfilled_demand)

    stock_df['stock_balance'] = stock_balances
    stock_df['missed_sales_qty'] = lost_sales_qty
    stock_df['outgoing_qty'] = fulfilled_demands  # outgoing_qty is now what could actually be fulfilled

    if current_status is not None:
        # check if we actually miss a productrevenue (i.e. by not having the product in stock), this is only true for active items
        if current_status == 'AUSLAUF':
            stock_df['missed_sales_qty'] = 0.

    #stock_df['stock_balance_value'] = stock_df['stock_balance'] * current_unitcost
    #stock_df['productrevenue'] = stock_df['outgoing_qty'] * current_netprice
    #stock_df['missed_productrevenue'] = stock_df['missed_sales_qty'] * current_netprice

    return stock_df

def run_chain_for_item(item_no, scenario_config=None):
    current_item_facts = get_item_facts(item_no)

    demand = get_demand_fc(item_no, scenario_config=scenario_config)
    incoming = get_incoming_pos(item_no, filterout_sammelpos = True)
    
    fc = build_full_forecast(
    available_stock=current_item_facts["if2.availableStock"].iloc[0],
    current_unitcost = None, #current_item_facts["if2.unitCost"].iloc[0],
    current_netprice = None, #current_item_facts['salesPriceNettoDE'].iloc[0],
    current_status = current_item_facts["id.itemStatusCode"].iloc[0],
    incoming_purchases_df=incoming,
    outgoing_demand_df=demand)
    
    fc['itemNo'] = item_no
    return fc


def forecast_multiple_items_parallel(
    items_list = [],
    max_workers=8,
    scenario_tag='',
    scenario_config=None
    ):
    args_list = []
    for item in items_list:
        args_list.append((item, scenario_config))

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for res in tqdm.tqdm(
            executor.map(lambda args: run_chain_for_item(*args), args_list),
            total=len(items_list)
        ):
            results.append(res)
    combined_df = pd.concat(results, ignore_index=True)
    cols = ['itemNo'] + [c for c in combined_df.columns if c != 'itemNo']
    combined_df = combined_df[cols]
    combined_df['scenario_tag'] = scenario_tag
    return combined_df

def get_base_items():
    query = f"""
        select id.itemNo as itemNo
        from analytics.item_dimensions id 
        left join analytics.item_facts  if2 on if2.itemNo = id.itemNo
        where masterItem = False AND setItem = False  AND length(itemNo) > 5 AND toInt64OrNull(itemNo) IS NOT NULL 
        AND ( id.itemStatusCode = 'AKTIV' OR (id.itemStatusCode = 'AUSLAUF' AND if2.availableStock > 0) OR (if2.openIncomingQuantity > 0)) OR (if2.availableStock > 0)
        """

    df =  pd.read_sql(query, dwh_db_conn2)
    df = df.sample(len(df))
    return df

dwh_client = Client(
    host=dwh_db_cfg["host"],
    user=dwh_db_cfg["user"],
    password=dwh_db_cfg["password"],
    database=dwh_db_cfg["schema"],
    port=9440,
    secure=True,
    verify=True,
)

def push_fc_to_dwh(final_df=None):
    expected_columns = [
        'itemNo',
        'week_ending_date',
        'scenario_tag',
        'incoming_qty',
        'avg_unit_cost',
        'outgoing_qty',
        'stock_balance',
        'missed_sales_qty'
    ]

    final_df['itemNo'] = final_df['itemNo'].fillna('').astype(str)
    final_df['scenario_tag'] = final_df['scenario_tag'].fillna('').astype(str)

    final_df = final_df[expected_columns].copy()
    final_df['week_ending_date'] = pd.to_datetime(final_df['week_ending_date'])
        
    # Convert DataFrame rows to list of tuples for insertion
    records = [tuple(row) for row in final_df.itertuples(index=False, name=None)]
    
    
    # Prepare INSERT query (note: do not use 'VALUES' here)
    insert_query = '''
    INSERT INTO analytics.business_forecast_itemlevel
    (itemNo, week_ending_date, scenario_tag, incoming_qty, avg_unit_cost, outgoing_qty,
     stock_balance, missed_sales_qty)
     VALUES
    '''
    
    # Execute the insert with data
    dwh_client.execute(insert_query, records)

def check_if_already_procssed(item_nos, scenario_tag):
    """
    Returns a set of itemNos (as int) that are already processed for the given scenario_tag.
    """
    if not item_nos:
        return set()

    # Convert all item_nos to int for SQL and comparison
    item_nos_int = [int(x) for x in item_nos]
    item_nos_str = ",".join([str(x) for x in item_nos_int])
    query = f"""
        SELECT DISTINCT itemNo FROM analytics.business_forecast_itemlevel
        WHERE itemNo IN ({item_nos_str}) AND scenario_tag = '{scenario_tag}'
    """
    df = pd.read_sql(query, dwh_db_conn2)
    # Ensure all returned itemNos are int for comparison
    return set(df['itemNo'].astype(int).tolist())

@app.command()
def main(
        scenario_tag: str = typer.Option('baseline_noSammelPOs_onlyStatisticalForecasts', help="Scenario tag for the forecast"),
        sample_n: int = typer.Option(-1, help="Number of itemNos to process"),
        single_item_no: str = typer.Option(None, help="Single itemNo to process (overrides sample_n)"),
        scenario_config: str = typer.Option(None, help="Path to scenario uplift YAML config")
        ):
    """
    Run bottom-up business forecast for a sample of items.
    """
    # Load config if provided
    config_data = None
    if scenario_config is not None:
        with open(scenario_config, "r") as f:
            config_data = yaml.safe_load(f)

    all_items = get_base_items()
    print("Found {} items to process".format(len(all_items)))
    if single_item_no is not None:
        items_to_process = all_items.loc[all_items.itemNo == f'{single_item_no}']
    elif sample_n > 0:
        items_to_process = all_items.sample(sample_n)
    else:
        items_to_process = all_items

    items_to_process = list(items_to_process.itemNo.astype(int).values)
    print("Subsampled to {} items for process".format(len(items_to_process)))

    chunksize = 4000  # Always use 200, but handle shorter lists gracefully

    idx = 0
    total = len(items_to_process)
    while idx < total:

        current_subsample = items_to_process[idx:idx + chunksize]
        already_processed = check_if_already_procssed(current_subsample, scenario_tag)
        current_subsample = [int(item) for item in current_subsample if int(item) not in already_processed]
        if not current_subsample:
            idx += chunksize
            continue

        df_result = forecast_multiple_items_parallel(
            current_subsample,
            scenario_tag=scenario_tag,
            scenario_config=config_data
        )
        #print(df_result.head(10))
        push_fc_to_dwh(df_result)
        idx += chunksize
        left = total - idx
        if left > 0:
            print(f"{left} items left")

if __name__ == "__main__":
    app()
