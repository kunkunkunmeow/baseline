import pandas as pd
from multiprocessing import Process, Manager
import pandas_gbq
import numpy as np
from tqdm import tqdm
import time
import logging

# Input global variables 
# Reformat code to accept these variables as input

# Project ID
project_id = "gum-eroski-dev"

# Set logger properties
logger = logging.getLogger('baseline_calculation')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('baseline.log')
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

# Define key baseline parameters
# Category level used to compute the baseline
bl_l = "section"

# Pull forward week
ext_day = 1

# Baseline % metrics
metrics = {
    'sale_amt': ['sale_amt_np', 'total_sale_amt'],
    'sale_qty': ['sale_qty_np', 'total_sale_qty'],
    'margin_amt': ['margin_amt_np', 'total_margin_amt']
}

# Function to load aggregate_weekly_transaction_summary data at a sku level from bigquery
def load_t1_from_bq():
    start_time = time.time()

    summary_sql = """
    SELECT date, sku_root_id , area, section, category, subcategory , segment , total_sale_amt, total_sale_qty , total_margin_amt , promo_flag_binary, sale_amt_promo_flag, sale_qty_promo_flag, margin_amt_promo_flag
    FROM `ETL.aggregate_weekly_transaction_summary`
    WHERE category = "CONFECCION MUJER"   """
    start = time.time()

    for i in tqdm(range(1), desc='loading summary table from bigquery'):
        summary_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of summary table from bigquery in {a} mins...".format(a=total_time))

    return summary_table


# Function to load aggregate_weekly_transaction_summary data at a sku level (for non-promotional periods) from bigquery
def load_t2_from_bq():
    start_time = time.time()

    weeklyagg_sql = """
    SELECT date, sku_root_id, area, section, category, subcategory, segment, sum(total_sale_amt) as sale_amt_np, sum(total_sale_qty) as sale_qty_np, sum(total_margin_amt) as margin_amt_np
    FROM `ETL.aggregate_weekly_transaction_to_sku`
    WHERE promo_flag = false
    AND category = "CONFECCION MUJER"
    group by date, sku_root_id, area, section, category, subcategory, segment """

    for i in tqdm(range(1), desc='loading weekly transaction table from bigquery'):
        weekly_agg = pandas_gbq.read_gbq(weeklyagg_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of summary table from bigquery in {a} mins...".format(a=total_time))

    return weekly_agg


# define function to aggregate table by defined level
def aggregate_np(weekly_agg, level):
    start_time = time.time()

    agg = weekly_agg.groupby(["date", level], as_index=False)['sale_amt_np', 'sale_qty_np', 'margin_amt_np'].sum()

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed aggregating non-promotional summary transactions in {a} mins...".format(a=total_time))

    return agg

# define function to determine % of total sale quantity 
def aggregate_sku(agg_np, level: str):
    """produce aggregated results of desired level and the change % of total sale quantity along the timeline
    Args:
        level(str): desired level for the sku to be grouped to
    Returns:
        agg_np_sku(pd.DataFrame): Final, preprocessed dataframe
    """
    # locate the group that needs to be aggregated
    sku_level = summary_table.loc[summary_table['sku_root_id'] == sku, level].iloc[0]

    # get the aggregated none promotion data for the group that the SKU belongs to
    agg_np_sku = agg_np[agg_np[level] == sku_level].sort_values(by=['date']).reset_index(drop=True)
    for metric in metrics.keys():
        agg_np_sku[f'{metric}_pct'] = 1
        for i in range(1, len(agg_np_sku)):
            if agg_np_sku.loc[i - 1, metrics[metric][0]] == 0:
                agg_np_sku.loc[i, f'{metric}_pct'] = np.nan
            else:
                agg_np_sku.loc[i, f'{metric}_pct'] = float(
                    agg_np_sku.loc[i, metrics[metric][0]] / agg_np_sku.loc[i - 1, metrics[metric][0]])
    return agg_np_sku


# define function to process baseline for one sku
def baseline_sku(frame, sku: str, summary_table, agg_np):
    """produce baseline and cannibalisation line for the sku
    Args:
        sku(str): sku_root_id
    Returns:
        final_df(pd.DataFrame): Final, preprocessed dataframe
    """
    logger.debug(f'{sku} - being processed')
    
    # get dataframe for the specific sku
    df_sku = summary_table[summary_table.sku_root_id == sku].sort_values(by=['date']).reset_index(drop=True)

    # produce baseline dataframe at desired level
    baseline = aggregate_sku(agg_np, bl_l)

    # merge baseline, cannibalisation and sku table
    table = pd.merge(df_sku[['date', 'sku_root_id', 'promo_flag_binary', 'total_sale_amt', 'total_sale_qty',
                             'total_margin_amt', 'sale_amt_promo_flag', 'sale_qty_promo_flag',
                             'margin_amt_promo_flag']],
                     baseline[['date', bl_l, 'sale_amt_pct', 'sale_qty_pct', 'margin_amt_pct']],
                     on=['date']).reset_index(drop=True)

    # define change_flag to be the reference of baseline and cannibalisation calculation
    # set 1 : for the first day on promotion
    # set 2: for the rest days on promotion after first day
    # set 3: for the extension days of baseline calculation
    table['change_flag'] = table['promo_flag_binary']
    for i in range(1, len(table)):
        if table.loc[i, 'change_flag'] == 1:
            table.loc[i, 'change_flag'] = 2
    for i in range(0, len(table) - 1):
        if table.loc[i, 'change_flag'] == 0 and table.loc[i + 1, 'change_flag'] == 2:
            table.loc[i + 1, 'change_flag'] = 1
    for i in range(0, len(table) - ext_day):
        for j in range(1, ext_day + 1):
            if table.loc[i, 'change_flag'] == 2 and table.loc[i + j, 'change_flag'] == 0:
                table.loc[i + j, 'change_flag'] = 3

    # produce baseline
    for metric in metrics.keys():
        table.loc[0, f'{metric}_bl'] = np.nan
        for i in range(1, len(table)):
            if table.loc[i, 'change_flag'] == 0:
                table.loc[i, f'{metric}_bl'] = np.nan
            if table.loc[i, 'change_flag'] == 1 and table.loc[i - 1, 'change_flag'] in [0, 3]:
                table.loc[i - 1, f'{metric}_bl'] = float(table.loc[i - 1, metrics[metric][1]])
            if table.loc[i, 'change_flag'] in [1, 2]:
                table.loc[i, f'{metric}_bl'] = round(table.loc[i - 1, f'{metric}_bl'] * table.loc[i, f'{metric}_pct'],
                                                     2)
    logger.debug(f'{sku} - completed baseline')

    # produce extended baseline
    for metric in metrics.keys():
        table.loc[0, f'{metric}_bl_ext'] = np.nan
        for i in range(1, len(table)):
            if table.loc[i, 'change_flag'] in [0, 1]:
                table.loc[i, f'{metric}_bl_ext'] = np.nan
            if table.loc[i, 'change_flag'] == 3 and table.loc[i - 1, 'change_flag'] == 2:
                table.loc[i - 1, f'{metric}_bl_ext'] = table.loc[i - 1, f'{metric}_bl']
            if table.loc[i, 'change_flag'] == 3:
                table.loc[i, f'{metric}_bl_ext'] = round(
                    table.loc[i - 1, f'{metric}_bl'] * table.loc[i, f'{metric}_pct'], 2)
    logger.debug(f'{sku} - completed pull forward baseline')

    # define incremental sale
    table['incremental_sale'] = pd.to_numeric(table['total_sale_amt']) - table['sale_amt_bl']
    table['incremental_margin'] = pd.to_numeric(table['total_margin_amt']) - table['margin_amt_bl']
    table['ROI_weight'] = table['incremental_sale'] / sum(table['incremental_sale'])

    # define final dataframe
    final_df = table[
        ['date', 'sku_root_id', 'promo_flag_binary', 'change_flag', 'total_sale_amt', 'sale_amt_bl', 'sale_amt_bl_ext',
         'total_sale_qty', 'sale_qty_bl', 'sale_qty_bl_ext', 'total_margin_amt', 'margin_amt_bl', 'margin_amt_bl_ext',
         'incremental_sale', 'incremental_margin', 'ROI_weight', 'sale_amt_promo_flag', 'sale_qty_promo_flag',
         'margin_amt_promo_flag']]

    logger.info(f'{sku} - completed baseline and pull forward calculation')
  
    frame.append(final_df)
    
if __name__ == "__main__":
    
    # Store final results here
    results_df = pd.DataFrame()
    
    start_time = time.time()

    logger.info("Loading input tables from Bigquery....")

    logger.info("Loading summary transaction tables from Bigquery....")
    summary_table = load_t1_from_bq()

    logger.info("Loading summary non-promotional transaction table from Bigquery....")
    weekly_agg = load_t2_from_bq()

    logger.info("Aggregating summary non-promotional transaction table at {a} level".format(a=bl_l))
    agg_np = aggregate_np(weekly_agg, bl_l)

    logger.info("Computing no. of unique in-scope skus")
    uniq_sku = list(summary_table['sku_root_id'].unique())
    logger.info("No. of in-scope skus: {a}".format(a=len(uniq_sku)))

#     pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    
#     for sku in uniq_sku:
#         pool.apply_async(baseline_sku, args=(sku, summary_table, agg_np), callback=collect_results)
#     pool.close()
#     pool.join()

        
    # do stuff with batch
    with Manager() as manager:
        frame = manager.list()  # <-- can be shared between processes.
        final_df = pd.DataFrame()
        processes = []
        batchsize = 50
        for i in range(0, len(uniq_sku), batchsize):
            start_time_batch = time.time()
            batch = uniq_sku[i:i+batchsize] # the result might be shorter than batchsize at the end
            processes[:] = []
            for sku in batch:
                p = Process(target=baseline_sku, args=(frame,sku,summary_table, agg_np))  # Passing the list
                p.start()
                processes.append(p)
            for p in processes:
                p.join()
            final_df = pd.concat(frame)
            final_df.reset_index(drop=True, inplace=True)
            results_df = results_df.append(final_df)
            total_time_batch = round((time.time() - start_time), 2)
            logger.info('Processing with batch size {a} took {b} secs...'.format(a=batchsize, b=total_time_batch))
            logger.info('Processed dataframe has {a} rows and {b} cols...'.format(a=final_df.shape[0], b=final_df.shape[1]))
            logger.info('Results dataframe has {a} rows and {b} cols...'.format(a=results_df.shape[0], b=results_df.shape[1]))
            final_df = final_df.iloc[0:0]
    #df = pd.DataFrame(results)

    # final_df.reset_index(drop=True, inplace=True)
    # 
    # for i in list(final_df.columns)[2:]:
    #     final_df[i] = pd.to_numeric(final_df[i])
    # 
    # final_df = final_df.where((pd.notnull(final_df)), None)
    total_time = round((time.time() - start_time) / 60, 1)
    logger.info('Completed baseline processing in {a} mins...'.format(a=total_time))

    # # upload the final dataframe onto '{sku} - final taBigquery
    # pandas_gbq.to_gbq(final_df, 'WIP.bl_multi2_1113', project_id=project_id, if_exists='replace')
    # logger.info('finished uploading onto Bigquery')
    # print('the whole process took', round((time.time() - start_time) / 60, 1), 'minutes.')
