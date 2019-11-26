import pandas as pd
from multiprocessing import Process, Manager
import pandas_gbq
import numpy as np
from tqdm import tqdm
import time
import logging

# Project ID
project_id = "gum-eroski-dev"

# flag whether to use negative cannibalisation value 
cb_np_flag = "all" # "all" or "positive"

start = time.time()

def load_bl_from_bq(project_id):
    start_time = time.time()

    summary_sql = f"""
    SELECT date, sku_root_id, {cb_l}, promo_flag_binary, incremental_sale_qty, cb_flag, cb_sale_amt, cb_sale_qty, cb_margin_amt
    FROM `WIP.baseline_dashboard`
    """

    for i in tqdm(range(1), desc='Loading table...'):
        baseline_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)

    return baseline_table
    
baseline_table = load_bl_from_bq(project_id)


baseline_table['cb_sale_amt'] = pd.to_numeric(baseline_table['cb_sale_amt'])
baseline_table['cb_sale_qty'] = pd.to_numeric(baseline_table['cb_sale_qty'])
baseline_table['cb_margin_amt'] = pd.to_numeric(baseline_table['cb_margin_amt'])
baseline_table['incremental_sale_qty'] = pd.to_numeric(baseline_table['incremental_sale_qty'])


cb_table = baseline_table.copy()

if cb_np_flag == "positive":
    num = cb_table._get_numeric_data()
    num[num < 0] = 0

agg_np = cb_table.groupby(["date",cb_l], as_index=False)['incremental_sale_qty','cb_sale_amt', 'cb_sale_qty', 'cb_margin_amt'].sum()
agg_np.columns = ['date', cb_l, 'ttl_inc_sale_qty','ttl_cb_sale_amt', 'ttl_cb_sale_qty', 'ttl_cb_margin_amt']

def cannibalisation(cb_table, date, level):
    table = cb_table[['date','sku_root_id', 'promo_flag_binary','incremental_sale_qty', level]][cb_table['date'] == date]
    
    df = pd.merge(table, agg_np, on=['date',level])

    df['cb_pct'] = df['incremental_sale_qty'] / df['ttl_inc_sale_qty']
    
    df['cb_sale_amt'] = df['ttl_cb_sale_amt']*df['cb_pct']
    df['cb_sale_qty'] = df['ttl_cb_sale_qty']*df['cb_pct']
    df['cb_margin_amt'] = df['ttl_cb_margin_amt']*df['cb_pct']
    
    final_df = df[['date', cb_l, 'sku_root_id','cb_sale_amt', 'cb_sale_qty', 'cb_margin_amt']]
    final_df.columns = ['date', cb_l, 'sku_root_id', 'ind_cb_sale', 'ind_cb_qty', 'ind_cb_margin']
    
    return final_df
  


