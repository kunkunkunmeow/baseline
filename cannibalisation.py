import pandas as pd
from multiprocessing import Process, Manager
import pandas_gbq
import numpy as np
from tqdm import tqdm
import time
import logging

# Project ID
project_id = "gum-eroski-dev"

# Define the cannibalisation level
cb_l = 'segment'

# flag whether to use negative cannibalisation value 
cb_np_flag = "all" # "all" or "positive"

# Set logger properties
logger = logging.getLogger('cannibalisation_calculation')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('cannibalisation.log')
fh.setLevel(logging.DEBUG)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

def load_bl_from_bq(project_id, cb_l):
    start_time = time.time()

    summary_sql = """
    SELECT date, sku_root_id, sku.segment, promo_flag_binary, incremental_sale_qty, cb_flag, cb_sale_amt, cb_sale_qty, cb_margin_amt
    FROM `WIP.baseline_dashboard`
    LEFT JOIN (SELECT sku_root_id, segment FROM `ETL.root_sku`) sku
    USING(sku_root_id)
    """ 

    for i in tqdm(range(1), desc='Loading table...'):
        baseline_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of baseline table from Bigquery {a} mins...".format(a=total_time))

    return baseline_table

# define the calculation of cannibalisation for certain date
def cannibalisation(frame, cb_table, date, level):
    table = cb_table[['date','sku_root_id', 'promo_flag_binary','incremental_sale_qty', level]][cb_table['date'] == date]
    
    df = pd.merge(table, agg_np, on=['date',level])

    df['cb_pct'] = df['incremental_sale_qty'] / df['ttl_inc_sale_qty']
    
    df['cb_sale_amt'] = df['ttl_cb_sale_amt']*df['cb_pct']
    df['cb_sale_qty'] = df['ttl_cb_sale_qty']*df['cb_pct']
    df['cb_margin_amt'] = df['ttl_cb_margin_amt']*df['cb_pct']
    
    final_df = df[['date', cb_l, 'sku_root_id','cb_sale_amt', 'cb_sale_qty', 'cb_margin_amt']]
    final_df.columns = ['date', cb_l, 'sku_root_id', 'ind_cb_sale', 'ind_cb_qty', 'ind_cb_margin']
    
    logger.info(f'{date} - completed cannibalisation calculation')
    frame.append(final_df)

if __name__ == "__main__":
    start_time = time.time()
    logger.info("Loading baseline tables from Bigquery....")
    baseline_table = load_bl_from_bq(project_id, cb_l)
    
    logger.info("Clean up baseline table ...")
    baseline_table['cb_sale_amt'] = pd.to_numeric(baseline_table['cb_sale_amt'])
    baseline_table['cb_sale_qty'] = pd.to_numeric(baseline_table['cb_sale_qty'])
    baseline_table['cb_margin_amt'] = pd.to_numeric(baseline_table['cb_margin_amt'])
    baseline_table['incremental_sale_qty'] = pd.to_numeric(baseline_table['incremental_sale_qty'])
    
    # options to ignore the negative values in the cannibalisation amount
    cb_table = baseline_table.copy()
    if cb_np_flag == "positive":
        num = cb_table._get_numeric_data()
        num[num < 0] = 0
    
    logger.info("aggreate the cannibalisation amount into the defined level")
    agg_np = cb_table.groupby(["date",cb_l], as_index=False)['incremental_sale_qty','cb_sale_amt', 'cb_sale_qty', 'cb_margin_amt'].sum()
    agg_np.columns = ['date', cb_l, 'ttl_inc_sale_qty','ttl_cb_sale_amt', 'ttl_cb_sale_qty', 'ttl_cb_margin_amt']
    
    #get unique dates 
    dates = list(baseline_table.date.unique())
    
    with Manager() as manager:
        frame = manager.list()  # <-- can be shared between processes.
#         processes = []
        
#         for i in range(0, len(dates)):
#              # Clear the processes list
#             processes[:] = []

        start_time = time.time()

        for date in dates:
            p = Process(target=cannibalisation, args=(frame, cb_table, date, cb_l))  # Passing the list
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

        output = pd.concat(frame)
        results_df = pd.concat([results_df, output], ignore_index=True, sort =False)
        results_df.reset_index(drop=True, inplace=True)
#         frame[:] = [] 

        total_time = round((time.time() - start_time_batch), 2)
        logger.debug('Processing took {b} secs...'.format(b=total_time))
        logger.info('Results dataframe has {a} rows and {b} cols...'.format(a=results_df.shape[0], b=results_df.shape[1]))
      
    # Convert all nulls to None
    results_df = results_df.where((pd.notnull(results_df)), None)
    
    total_time = round((time.time() - start_time) / 60, 1)
    logger.info('Completed baseline processing in {a} mins...'.format(a=total_time))
   
    pandas_gbq.to_gbq(results_df, 'WIP.cannibalisation_test', project_id=project_id, if_exists='replace')

