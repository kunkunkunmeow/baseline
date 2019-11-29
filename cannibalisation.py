# BASED ON INCREMENTAL SALE 

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

# Append or replace destination table (either 'append' or 'replace')
bl_table_config = 'replace'

# Set batch size
batchsize = 50

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

def load_section_from_bq(project_id):
    start_time = time.time()

    summary_sql = """
    SELECT distinct section
    FROM `baseline_performance.baseline` 
    LEFT JOIN (SELECT sku_root_id, section FROM `ETL.root_sku`) 
    USING(sku_root_id)
    WHERE SECTION = "BEBIDAS" """
    start = time.time()

    for i in tqdm(range(1), desc='Loading table...'):
        section_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of distinct sections table from Bigquery {a} mins...".format(a=total_time))

    return section_table

def load_bl_from_bq(project_id, section, level):
    start_time = time.time()

    summary_sql = """
    SELECT cast(DATE(date) AS DATE) AS date, sku_root_id, section, segment, promo_flag_binary, change_flag,
            cast(incremental_qty as NUMERIC) AS incremental_qty, CAST(sale_amt_bl AS NUMERIC) AS sale_amt_bl, CAST(sale_qty_bl  AS NUMERIC) AS sale_qty_bl, CAST(margin_amt_bl  AS NUMERIC) AS margin_amt_bl ,
            CAST(sale_amt_pct AS NUMERIC) AS sale_amt_pct, CAST(sale_qty_pct AS NUMERIC) AS sale_qty_pct, CAST(margin_amt_pct AS NUMERIC) AS margin_amt_pct, CAST(total_sale_amt AS NUMERIC) AS total_sale_amt, CAST(total_sale_qty  AS NUMERIC) AS total_sale_qty,
            CAST(total_margin_amt  AS NUMERIC) AS total_margin_amt 
    FROM `baseline_performance.baseline`
    LEFT JOIN (SELECT sku_root_id, section, segment FROM `ETL.root_sku`) 
    USING(sku_root_id)
    WHERE section = "%s"   """ %(section)

    for i in tqdm(range(1), desc='Loading table...'):
        baseline_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of baseline table from Bigquery {a} mins...".format(a=total_time))

    return baseline_table

def cb_sku (frame, sku, summary_table):
    logger.debug(f'{sku} - being processed')

    # get dataframe for the specific sku
    table = summary_table[summary_table.sku_root_id == sku].sort_values(by=['date']).reset_index(drop=True)

    # define cb_flag to be the reference of cannibalisation calculation 
    # set 1: for the first date where there's no promotion for that sku but promotion in at least one sku in the cb_l level 
    # set 2: for the rest days where there's no promotion for that sku but promotion in at least one sku in the cb_l level 
    # set 4: days where the sku is on promotion 
    for i in range(0,len(table)):
        if table.loc[i,'cb_flag_temp'] ==0 and table.loc[i,'promo_flag_binary'] == 0:
            table.loc[i,'cb_flag'] = 0
        if table.loc[i,'cb_flag_temp'] >0 and table.loc[i,'promo_flag_binary'] == 0:
            table.loc[i,'cb_flag'] = 2
        if table.loc[i,'change_flag'] ==3 and table.loc[i,'cb_flag_temp'] >0:
            table.loc[i,'cb_flag'] = 1
        if table.loc[i,'promo_flag_binary'] == 1:
            table.loc[i,'cb_flag'] = 4
    for i in range(1,len(table)):    
        if table.loc[i,'cb_flag'] == 2 and table.loc[i-1,'cb_flag'] == 0:
            table.loc[i,'cb_flag'] = 1
    
    
    #produce cannibalisation baseline
    metrics = ['sale_amt', 'sale_qty', 'margin_amt']
    for metric in metrics:
        for i in range(0, len(table)):
            if i==0:                                                                                 
                if table.loc[i, 'cb_flag'] in [1,2]:
                    table.loc[i, f'{metric}_cb_bl'] = float(table.loc[i, f'total_{metric}'])
                else:
                    table.loc[i, f'{metric}_cb_bl'] = np.nan
            else:
                if table.loc[i, 'cb_flag'] in [0, 4]:
                    table.loc[i, f'{metric}_cb_bl'] = np.nan
                if table.loc[i, 'cb_flag'] == 1 and table.loc[i - 1, 'cb_flag'] == 4:
                    table.loc[i, f'{metric}_cb_bl'] = round(float(table.loc[i - 1, f'{metric}_bl']) * float(table.loc[i, f'{metric}_pct']),
                                                        2)
                if table.loc[i, 'cb_flag'] == 1 and table.loc[i - 1, 'cb_flag'] == 0:
                    table.loc[i, f'{metric}_cb_bl'] = round(float(table.loc[i, f'total_{metric}']),                                                                          2)
                if table.loc[i, 'cb_flag'] ==2:
                    table.loc[i, f'{metric}_cb_bl'] = round(float(table.loc[i - 1, f'{metric}_cb_bl']) * float(table.loc[i, f'{metric}_pct']),
                                                        2)
    
    # define cannibalisation amount 
    for metric in metrics:
        table[f'cb_{metric}'] = table[f'{metric}_cb_bl'] - pd.to_numeric(table[f'total_{metric}'], errors='coerce')
    
    final_df = table[['date', 'sku_root_id', 'section', 'segment', 'promo_flag_binary', 'incremental_qty', 
                      'cb_flag', 'sale_amt_cb_bl', 'sale_qty_cb_bl', 'margin_amt_cb_bl', 'cb_sale_amt', 'cb_sale_qty', 'cb_margin_amt']]
    
    logger.info(f'{sku} - completed cb baseline calculation')
    
    frame.append(final_df)


# define the calculation of cannibalisation for certain date
def cannibalisation(frame, agg_np, cb_table, cb_l, cb_level):
    table = cb_table[['date','sku_root_id', 'promo_flag_binary', 'cb_flag', 'sale_amt_cb_bl', 'sale_qty_cb_bl', 'margin_amt_cb_bl',
                      'incremental_qty', cb_l]][cb_table[cb_l] == cb_level]

    agg_np_cb = agg_np[agg_np[cb_l] == cb_level]

    df = pd.merge(table, agg_np_cb, on=['date',cb_l])

    df['cb_pct'] = df['incremental_qty']/ df['ttl_inc_sale_qty']
    df.loc[~np.isfinite(df['cb_pct']), 'cb_pct'] = 0
    
    df['cb_sale_amt'] = df['ttl_cb_sale_amt']*df['cb_pct']
    df['cb_sale_qty'] = df['ttl_cb_sale_qty']*df['cb_pct']
    df['cb_margin_amt'] = df['ttl_cb_margin_amt']*df['cb_pct']
    
    final_df = df[['date', cb_l, 'cb_flag', 'sale_amt_cb_bl', 'sale_qty_cb_bl', 'margin_amt_cb_bl',
                   'sku_root_id','cb_sale_amt', 'cb_sale_qty', 'cb_margin_amt']]
    final_df.columns = ['date', cb_l, 'cb_flag', 'sale_amt_cb_bl', 'sale_qty_cb_bl', 'margin_amt_cb_bl',
                        'sku_root_id', 'ind_cb_sale', 'ind_cb_qty', 'ind_cb_margin']
    
    logger.info(f'{cb_level} - completed cannibalisation calculation')
    frame.append(final_df)

if __name__ == "__main__":
    start_time = time.time()
    logger.info("Loading baseline tables from Bigquery....")
    section_table = load_section_from_bq(project_id)

    # Unique sections in category include
    unique_sections = list(section_table["section"].unique())
    logger.info("Unique sections include:")
    for section in unique_sections: logger.info("{a}".format(a=section))

    # Loop through sections
    for i_sec in range(0, len(unique_sections)):
        section_start_time = time.time()
        section = unique_sections[i_sec]

        logger.info("Processing section {a}...".format(a=section))

        # load baseline table for each section     
        logger.info("Loading summary transaction table from Bigquery....")
        baseline_table = load_bl_from_bq(project_id, section, cb_l)
    
        logger.info("Clean up baseline table ...")
        # baseline_table['cb_sale_amt'] = pd.to_numeric(baseline_table['cb_sale_amt'])
        # baseline_table['cb_sale_qty'] = pd.to_numeric(baseline_table['cb_sale_qty'])
        # baseline_table['cb_margin_amt'] = pd.to_numeric(baseline_table['cb_margin_amt'])
        # baseline_table['incremental_qty'] = pd.to_numeric(baseline_table['incremental_qty'])
        baseline_table['incremental_qty'][baseline_table['incremental_qty']<0] =0

        logger.info("Defining cannabalisation flag at {a} level".format(a=cb_l))
        cb_flag= baseline_table[['date', cb_l, 'promo_flag_binary']].groupby(["date", cb_l], as_index=False).sum()
        cb_flag.columns = ['date',cb_l,'cb_flag_temp']
        summary_table = pd.merge(baseline_table, cb_flag, on=['date',cb_l])
    
        #get unique dates 
        logger.info("Computing no. of unique in-scope dates")
        dates = list(baseline_table.date.unique())

        logger.info("Computing no. of unique in-scope cb_l")
        unique_cb_l = list(baseline_table[cb_l].unique())

        logger.info("Computing no. of unique in-scope skus")
        uniq_sku = list(summary_table['sku_root_id'].unique())
        logger.info("No. of in-scope skus: {a}".format(a=len(uniq_sku)))
    
        # Store the baseline results
        baseline_cb_df = pd.DataFrame()
        results_df = pd.DataFrame()

        with Manager() as manager:
            frame = manager.list()  # <-- can be shared between processes.
            processes = []

            for i in range(0, len(uniq_sku), batchsize):
                # Clear the processes list
                processes[:] = []

                start_time_batch = time.time()
                batch = uniq_sku[i:i+batchsize] # the result might be shorter than batchsize at the end

                for sku in batch:
                    p = Process(target=cb_sku, args=(frame,sku,summary_table))  # Passing the list
                    p.start()
                    processes.append(p)
                for p in processes:
                    p.join()
                output = pd.concat(frame)
                baseline_cb_df = pd.concat([baseline_cb_df, output], ignore_index=True, sort =False)
                baseline_cb_df.reset_index(drop=True, inplace=True)
                frame[:] = [] 

                total_time_batch = round((time.time() - start_time_batch), 2)
                logger.debug('Processing with batch size {a} took {b} secs...'.format(a=batchsize, b=total_time_batch))

                logger.info('Results dataframe has {a} rows and {b} cols...'.format(a=baseline_cb_df.shape[0], b=baseline_cb_df.shape[1]))

            # options to ignore the negative values in the cannibalisation amount
            cb_table = baseline_cb_df.copy()
            cb_table['incremental_qty'] = pd.to_numeric(cb_table['incremental_qty'])
            if cb_np_flag == "positive":
                num = cb_table._get_numeric_data()
                num[num < 0] = 0
        
            logger.info("aggreate the cannibalisation amount into the defined level")
            agg_np = cb_table.groupby(['date',cb_l], as_index=False)['incremental_qty','cb_sale_amt', 'cb_sale_qty', 'cb_margin_amt'].sum()
            agg_np.columns = ['date', cb_l, 'ttl_inc_sale_qty','ttl_cb_sale_amt', 'ttl_cb_sale_qty', 'ttl_cb_margin_amt']


            for i in range(0,len(unique_cb_l), batchsize):
                # Clear the processes list
                processes[:] = []

                start_time_batch = time.time()
                batch = unique_cb_l[i:i+batchsize] # the result might be shorter than batchsize at the end

                for cb_level in batch:
                    p = Process(target=cannibalisation, args=(frame, agg_np, cb_table, cb_l, cb_level))  # Passing the list
                    p.start()
                    processes.append(p)
                for p in processes:
                    p.join()

                output = pd.concat(frame)
                results_df = pd.concat([results_df, output], ignore_index=True, sort =False)
                results_df.reset_index(drop=True, inplace=True)
                frame[:] = [] 

                total_time_batch = round((time.time() - start_time_batch), 2)
                logger.debug('Processing with batch size {a} took {b} secs...'.format(a=batchsize, b=total_time_batch))

                logger.info('Results dataframe has {a} rows and {b} cols...'.format(a=results_df.shape[0], b=results_df.shape[1]))
        
        
        # Convert all nulls to None
        results_df = results_df.where((pd.notnull(results_df)), None)

        total_time = round((time.time() - section_start_time) / 60, 1)
        logger.info('Completed cannibalisation processing in {a} mins...'.format(a=total_time))
        
        # upload the final dataframe onto Bigquery
        logger.info('Uploading cannibalisation table to Bigquery...')

        if (i_sec == 0):
            pandas_gbq.to_gbq(results_df, 'WIP.cb1129', project_id=project_id, if_exists=bl_table_config)
        else:
            pandas_gbq.to_gbq(results_df, 'WIP.cb1129', project_id=project_id, if_exists='append')

        logger.info('Completed upload of section baseline to Bigquery...')
    
    total_time = round((time.time() - start_time) / 60, 1)
    logger.info('Completed baseline processing in {a} mins...'.format(a=total_time))
