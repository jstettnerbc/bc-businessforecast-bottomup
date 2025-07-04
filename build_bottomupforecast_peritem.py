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

# Hardcoded path for manual forecast Excel files
MANUALFORECAST_FILES = ["data/20250701_manual_demand_estimates.xlsx", "data/250702_N-Artikel_Round_2.xlsx"]

# Read all Excel files and concatenate them into a single DataFrame
manual_forecast_dfs = []
for file_path in MANUALFORECAST_FILES:
    if os.path.exists(file_path):
        try:
            df = pd.read_excel(file_path)
            manual_forecast_dfs.append(df)
        except Exception as e:
            print(f"Warning: Could not read {file_path}: {e}")

if manual_forecast_dfs:
    manual_forecast_df = pd.concat(manual_forecast_dfs, ignore_index=True)
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


def get_base_info_per_item(item_no):
    """
    Fetches base information for a given item number from the item_dimensions table.
    Returns a DataFrame with the item's details.
    """
    item_no = str(item_no).strip()
    query = """ select brand, wareGroupLevel0, wareGroupLevel1, webshop_isListedOnline,  firstDateOnlineListed, salesStartingDate from  analytics.item_dimensions id
                where itemNo = '{item_no}'
            """
    return pd.read_sql(query.format(item_no=item_no), dwh_db_conn2)


def get_demand_fc(item_no, scenario_config=None, newbie_salesstart_offset=None):
    item_info = get_base_info_per_item(item_no)

    query = f"""
    select month_startdate, forecasted_demand_adjusted, forecasted_demand_statistical
              from analytics.diskover_demand_forecast fc
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
            item_brand = item_info['brand'].iloc[0]
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

    # DATABI-506: apply offset for newbie articles, i.e. shift their starting date on the demand side
    sales_start_date = pd.to_datetime(item_info['salesStartingDate'].iloc[0])
    webshop_is_listed = bool(item_info['webshop_isListedOnline'].iloc[0])
    if (
        webshop_is_listed == False
        and sales_start_date > today - pd.Timedelta(days=180)
    ):
        # If the item is not listed online AND has a reasonable salesStartingDate, we null the demand for the first weeks
        if not weekly_forecast.empty:
            weekly_forecast.loc[
                weekly_forecast['week_ending_date'] < (sales_start_date + pd.Timedelta(days=newbie_salesstart_offset)),
                'forecasted_demand_adjusted'
            ] = 0

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


def get_incoming_pos(item_no, filterout_sammelpos=False, add_overdue_pos=True, sample_strongly_overdue=True) :
    query = f"""
        select documentNo, orderStatus, itemNo, quantityOrdered-quantityReceived as quantityOpen, unitCostThisPurchaseLine, expectedReceiptDate
        from reporting.open_purchaseorder_lines where itemNo = '{item_no}' and expectedReceiptDate < '2030-01-01' order by expectedReceiptDate"""

    inc = pd.read_sql(query, dwh_db_conn2)

    if inc.empty :
        # Create a weekly date range for 16 months (~69 weeks) starting from current week ending Sunday
        start_date = pd.Timestamp(datetime.today())
        start_week_end = start_date + pd.offsets.Week(weekday=6)  # Sunday
        periods = 53  # about 24 months in weeks
        dates = pd.date_range(start=start_week_end.normalize(), periods=periods, freq='W-SUN')
        inc_resampled = pd.DataFrame({
            'week_ending_date' : dates,
            'quantityOpen' : 0,
            'unitCostThisPurchaseLine' : 0
        })
    else :
        if filterout_sammelpos :
            inc = inc[~inc['documentNo'].isin(sammel_PO_ids)]

        inc['expectedReceiptDate'] = pd.to_datetime(inc['expectedReceiptDate']).dt.normalize()
        inc['week_ending_date'] = inc['expectedReceiptDate'] + pd.offsets.Week(weekday=6)
        inc['week_ending_date'] = inc['week_ending_date'].dt.normalize()  # remove time part and milliseconds

        today = pd.Timestamp(datetime.today()).normalize()
        this_week_end = today + pd.offsets.Week(weekday=6)

        # Masks for overdue types
        split_overdue_and_strongly_overdue = 20
        strongly_overdue_mask = inc['expectedReceiptDate'] < (today - pd.Timedelta(days=split_overdue_and_strongly_overdue))
        overdue_mask = (inc['expectedReceiptDate'] < today) & (
                inc['expectedReceiptDate'] >= (today - pd.Timedelta(days=split_overdue_and_strongly_overdue)))
        not_overdue_mask = ~(strongly_overdue_mask | overdue_mask)

        strongly_overdue = inc[strongly_overdue_mask]
        overdue = inc[overdue_mask]
        not_overdue = inc[not_overdue_mask]

        # Strongly overdue: sample a random day between today and today+90, assign full qty to that week if flag is set
        sampled_rows = []
        if sample_strongly_overdue and not strongly_overdue.empty :
            rng = np.random.default_rng()
            for _, row in strongly_overdue.iterrows() :
                random_offset = rng.integers(0, 91)
                sampled_date = today + pd.Timedelta(days=int(random_offset))
                week_ending = (sampled_date + pd.offsets.Week(weekday=6)).normalize()
                sampled_rows.append({
                    'week_ending_date' : week_ending,
                    'quantityOpen' : row['quantityOpen'],
                    'unitCostThisPurchaseLine' : row['unitCostThisPurchaseLine']
                })
        # If not sampling, strongly overdue are ignored
        sampled_df = pd.DataFrame(sampled_rows)
        if not sampled_df.empty :
            sampled_df = sampled_df.groupby('week_ending_date').agg({
                'quantityOpen' : 'sum',
                'unitCostThisPurchaseLine' : 'mean'
            }).reset_index()
        else :
            sampled_df = pd.DataFrame(columns=['week_ending_date', 'quantityOpen', 'unitCostThisPurchaseLine'])

        # Overdue but not strongly overdue: sum qty, avg cost, assign to first week if flag is set
        overdue_rows = []
        if add_overdue_pos and not overdue.empty :
            overdue_qty = overdue['quantityOpen'].sum()
            overdue_cost = overdue['unitCostThisPurchaseLine'].mean()
            overdue_rows.append({
                'week_ending_date' : this_week_end.normalize(),
                'quantityOpen' : overdue_qty,
                'unitCostThisPurchaseLine' : overdue_cost
            })
        overdue_df = pd.DataFrame(overdue_rows)

        # Not overdue: group as before
        not_overdue_grouped = not_overdue.groupby('week_ending_date').agg({
            'quantityOpen' : 'sum',
            'unitCostThisPurchaseLine' : 'mean'
        }).reset_index()

        # Combine all
        inc_grouped = pd.concat([sampled_df, overdue_df, not_overdue_grouped], ignore_index=True)
        inc_grouped = inc_grouped.groupby('week_ending_date').agg({
            'quantityOpen' : 'sum',
            'unitCostThisPurchaseLine' : 'mean'
        })

        start_date = pd.Timestamp(datetime.today())
        start_week_end = (start_date + pd.offsets.Week(weekday=6)).normalize()
        periods = 53  # 24 months approx in weeks
        full_range = pd.date_range(start=start_week_end, periods=periods, freq='W-SUN')

        inc_resampled = inc_grouped.reindex(full_range, fill_value=0)

        # For unitCostThisPurchaseLine, fill with NaN or with previous non-null value if any
        if inc_resampled['unitCostThisPurchaseLine'].isna().all() :
            inc_resampled['unitCostThisPurchaseLine'] = 0
        else :
            inc_resampled['unitCostThisPurchaseLine'].fillna(method='ffill', inplace=True)
            inc_resampled['unitCostThisPurchaseLine'].fillna(0, inplace=True)

        inc_resampled = inc_resampled.reset_index().rename(columns={'index' : 'week_ending_date'})

    # Filter: Only keep weeks >= this week's Sunday
    this_week_end = pd.Timestamp(datetime.today()) + pd.offsets.Week(weekday=6)
    inc_resampled = inc_resampled[inc_resampled['week_ending_date'] >= this_week_end.normalize()].reset_index(drop=True)
    print(inc_resampled)
    exit()

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

def _calculate_stock_measures(stock_df, available_stock):
    """
    Efficiently calculate stock_balance, fulfilled_demand, and lost_qty for each row.
    Returns three lists: stock_balances, lost_sales_qty, fulfilled_demands.
    """
    stock_balance = available_stock
    stock_balances = []
    lost_sales_qty = []
    fulfilled_demands = []

    for row in stock_df.itertuples(index=False):
        stock_balance += row.incoming_qty
        fulfilled_demand = min(row.planned_outgoing_qty, stock_balance)
        stock_balance -= fulfilled_demand
        lost_qty = row.planned_outgoing_qty - fulfilled_demand

        lost_sales_qty.append(lost_qty)
        stock_balances.append(stock_balance)
        fulfilled_demands.append(fulfilled_demand)

    return stock_balances, lost_sales_qty, fulfilled_demands

def build_full_forecast(
    available_stock,
    current_unitcost,
    current_netprice,
    current_status,
    incoming_purchases_df,
    outgoing_demand_df,
    realize_potential_factor=0.0
):
    incoming_summary = incoming_purchases_df.set_index('week_ending_date').rename(columns={'quantityOpen': 'incoming_qty', 'unitCostThisPurchaseLine': 'avg_unit_cost'}).copy()
    outgoing_qty = outgoing_demand_df.set_index('week_ending_date')[['forecasted_demand_adjusted']].rename(columns={'forecasted_demand_adjusted': 'planned_outgoing_qty'}).copy()
    stock_df = pd.merge(incoming_summary, outgoing_qty, left_index=True, right_index=True, how='outer').fillna(0)
    stock_df = stock_df.sort_index().reset_index()
    this_week_end = pd.Timestamp(datetime.today()) + pd.offsets.Week(weekday=6)
    stock_df = stock_df[stock_df['week_ending_date'] >= this_week_end.normalize()].reset_index(drop=True)

    # Initial calculation
    stock_balances, lost_sales_qty, fulfilled_demands = _calculate_stock_measures(stock_df, available_stock)
    stock_df['stock_balance'] = stock_balances
    stock_df['missed_sales_qty'] = lost_sales_qty
    stock_df['outgoing_qty'] = fulfilled_demands

    if current_status is not None:
        if current_status == 'AUSLAUF':
            stock_df['missed_sales_qty'] = 0.


    # Realize potential missed sales as additional incoming qty, then recalculate
    if realize_potential_factor > 0:
        stock_df['incoming_qty'] = stock_df['incoming_qty'] + stock_df['missed_sales_qty'] * realize_potential_factor
        stock_balances, lost_sales_qty, fulfilled_demands = _calculate_stock_measures(stock_df, available_stock)
        stock_df['stock_balance'] = stock_balances
        stock_df['missed_sales_qty'] = lost_sales_qty
        stock_df['outgoing_qty'] = fulfilled_demands


    return stock_df

def run_chain_for_item(
    item_no,
    scenario_config=None,
    realize_potential_factor=0.0,
    newbie_salesstart_offset=None,
    add_overdue_pos=True,
    sample_strongly_overdue=True
):
    current_item_facts = get_item_facts(item_no)

    demand = get_demand_fc(item_no, scenario_config=scenario_config, newbie_salesstart_offset=newbie_salesstart_offset)
    incoming = get_incoming_pos(
        item_no,
        filterout_sammelpos=True,
        add_overdue_pos=add_overdue_pos,
        sample_strongly_overdue=sample_strongly_overdue
    )

    fc = build_full_forecast(
        available_stock=current_item_facts["if2.availableStock"].iloc[0],
        current_unitcost = None, #current_item_facts["if2.unitCost"].iloc[0],
        current_netprice = None, #current_item_facts['salesPriceNettoDE'].iloc[0],
        current_status = current_item_facts["id.itemStatusCode"].iloc[0],
        incoming_purchases_df=incoming,
        outgoing_demand_df=demand,
        realize_potential_factor=realize_potential_factor
    )

    fc['itemNo'] = item_no
    return fc


def forecast_multiple_items_parallel(
    items_list = [],
    max_workers=8,
    scenario_tag='',
    scenario_config=None,
    realize_potential_factor=0.0,
    newbie_salesstart_offset=None,
    add_overdue_pos=True,
    sample_strongly_overdue=True
    ):
    args_list = []
    for item in items_list:
        args_list.append((
            item,
            scenario_config,
            realize_potential_factor,
            newbie_salesstart_offset,
            add_overdue_pos,
            sample_strongly_overdue
        ))

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
    insert_query = """
    INSERT INTO analytics.business_forecast_itemlevel
    (itemNo, week_ending_date, scenario_tag, incoming_qty, avg_unit_cost, outgoing_qty,
     stock_balance, missed_sales_qty)
     VALUES
     """

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
        scenario_config: str = typer.Option(None, help="Path to scenario uplift YAML config"),
        realize_potential_factor: float = typer.Option(0.0, help="Fraction [0-1] of missed sales to realize as additional incoming qty"),
        newbie_salesstart_offset: int = typer.Option(0., help="Offset in days for newbie sales start (null demand for first N days after sales start)"),
        add_overdue_pos: bool = typer.Option(True, help="Add overdue PO quantities to first week"),
        sample_strongly_overdue_pos: bool = typer.Option(True, help="Sample strongly overdue POs between today and today+90, otherwise ignore")
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
        #already_processed = check_if_already_procssed(current_subsample, scenario_tag)
        #current_subsample = [int(item) for item in current_subsample if int(item) not in already_processed]
        if not current_subsample:
            idx += chunksize
            continue

        df_result = forecast_multiple_items_parallel(
            current_subsample,
            scenario_tag=scenario_tag,
            scenario_config=config_data,
            realize_potential_factor=realize_potential_factor,
            newbie_salesstart_offset=newbie_salesstart_offset,
            add_overdue_pos=add_overdue_pos,
            sample_strongly_overdue=sample_strongly_overdue_pos
        )

        push_fc_to_dwh(df_result)
        idx += chunksize
        left = total - idx
        if left > 0:
            print(f"{left} items left")

if __name__ == "__main__":
    app()
