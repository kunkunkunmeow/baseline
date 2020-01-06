import pandas as pd
from multiprocessing import Process, Manager
import pandas_gbq
import numpy as np
from tqdm import tqdm
import time
import logging
from datetime import timedelta  
import baseline_query

# Input global variables 
# Reformat code to accept these variables as input

# Project ID
project_id = "gum-eroski-dev"
dataset_id = "baseline"

# Define key baseline parameters
# Category level used to compute the baseline
# Need to be section level or below (i.e category, subcategory, etc)
bl_l = "section"

# Scope for the baseline (at an area level)
bl_s = "ALIMENTACION"


# Append or replace destination table (either 'append' or 'replace')
bl_table_config = 'replace'

# Pull forward week
ext_week = 4


# Set batch size
batchsize = 100

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

# Function to load distinct section data at a sku level from bigquery
def load_section_from_bq(area, project_id):
    start_time = time.time()

    summary_sql = """
    SELECT distinct section
    FROM `ETL.root_sku` 
    WHERE area = "{area}"
    """.format(area = area)
    
    start = time.time()

    for i in tqdm(range(1), desc='Loading table...'):
        section_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of distinct sections table from Bigquery {a} mins...".format(a=total_time))

    return section_table


# Function to load aggregate_weekly_transaction_summary data at a sku level from bigquery
def load_promo_from_bq(area, section, project_id):
    start_time = time.time()

    summary_sql = """
    SELECT date, sku_root_id , {bl_l} , promo_id, promo_year, promo_mechanic, discount_depth, no_to_pay, no_to_buy, total_sale_qty, s_prev_bl_qty, pf_after_bl_qty
    FROM `ETL.aggregate_promo_to_sku_summary`
    WHERE section = "{section}"
    AND area = "{area}" 
    """.format(bl_l = bl_l, section = section, area = area)
    start = time.time()

    for i in tqdm(range(1), desc='Loading table...'):
        summary_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of summary table from Bigquery in {a} mins...".format(a=total_time))

    return summary_table


# Function to load aggregate_weekly_transaction_summary data at a sku level (for non-promotional periods) from bigquery
def load_np_from_bq(area, section, project_id):
    start_time = time.time()

    weeklyagg_sql = """
    SELECT date, sku_root_id,  {bl_l}, sum(total_sale_qty) as sale_qty_np
    FROM `ETL.aggregate_weekly_transaction_to_sku`
    WHERE promo_flag = false
    AND section =  "{section}"
    AND area = "{area}" 
    group by date, sku_root_id, {bl_l} """.format(bl_l = bl_l, section = section, area = area)

    for i in tqdm(range(1), desc='Loading table...'):
        weekly_agg = pandas_gbq.read_gbq(weeklyagg_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of summary table from Bigquery in {a} mins...".format(a=total_time))

    return weekly_agg


# define function to aggregate table by defined level
def aggregate_np(weekly_agg, level):
    start_time = time.time()

    agg = weekly_agg.groupby(["date", level], as_index=False)['sale_qty_np'].sum()

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed aggregating non-promotional summary transactions in {a} mins...".format(a=total_time))

    return agg



# get the percentage changes for all baseline level parameters
def baseline_pct (frame, parameter:str, agg_np, bl_l):        
    # get the aggregated none promotion data for the group that the SKU belongs to
    df = agg_np[agg_np[bl_l] == parameter].sort_values(by = ['date']).reset_index(drop=True)
  
    df['sale_qty_pct'] = 1
    for i in range(1,len(df)):
        if df.loc[i-1,'sale_qty_np'] == 0:
            df.loc[i,'sale_qty_pct'] = np.nan
        else:
            df.loc[i,'sale_qty_pct'] = float(df.loc[i,'sale_qty_np']/df.loc[i-1,'sale_qty_np'])
    
    logger.info(f'{parameter} - completed baseline perc change calculation')
    
    frame.append(df)

# define function to process baseline for one sku
def baseline_id(frame, id: str, summary_table, baseline_ref, bl_l, ext_week, section):
    """produce baseline and cannibalisation line for the sku
    Args:
        sku(str): sku_root_id
    Returns:
        final_df(pd.DataFrame): Final, preprocessed dataframe
    """
    logger.debug(f'{id} - being processed')
    
    
    # get dataframe for the specific sku
    df_id = summary_table[summary_table.uniq_id == id].sort_values(by=['date']).reset_index(drop=True)
    

    # define change_flag to be the reference of baseline calculation
    # set 1 : for the first day on promotion
    # set 2: for the rest days on promotion after first day
    # set 3: for the extension days of baseline calculation
    df_id.loc[0, 'change_flag'] = 1
    for i in range(1, len(df_id)):
        df_id.loc[i, 'change_flag'] = 2
    for i in range(len(df_id), len(df_id) + ext_week):
        df_id.loc[i, 'date'] = df_id.loc[i-1, 'date'] + timedelta(days=7)
        df_id.loc[i, 'sku_root_id'] = df_id.loc[i-1, 'sku_root_id']
        df_id.loc[i, 'promo_id'] = df_id.loc[i-1, 'promo_id']
        df_id.loc[i, 'promo_year'] = df_id.loc[i-1, 'promo_year']
        df_id.loc[i, 'promo_mechanic'] = df_id.loc[i-1, 'promo_mechanic']
        df_id.loc[i, 'discount_depth'] = df_id.loc[i-1, 'discount_depth']
        df_id.loc[i, 'no_to_buy'] = df_id.loc[i-1, 'no_to_buy']
        df_id.loc[i, 'no_to_pay'] = df_id.loc[i-1, 'no_to_pay']
        df_id.loc[i, 'uniq_id'] = df_id.loc[i-1, 'uniq_id']
        df_id.loc[i, 'change_flag'] = 3     

    # locate the group that needs to be aggregated
    baseline_level = df_id[bl_l].iloc[0]
    
    # produce baseline dataframe at desired level
    baseline = baseline_ref[baseline_ref[bl_l] == baseline_level]

    # merge baseline and sku table
    table = pd.merge(df_id[['date', 'uniq_id', 'sku_root_id', 'promo_id', 'promo_year', 'promo_mechanic', 'discount_depth', 'no_to_pay','no_to_buy','change_flag','total_sale_qty', 's_prev_bl_qty', 'pf_after_bl_qty']],
                     baseline[['date', 'sale_qty_pct']],
                     on=['date']).reset_index(drop=True)
    
                                        
    # produce baseline + extended_baseline
    for i in range(0, len(table)):
        if table.loc[i, 'change_flag'] == 1:
            table.loc[i, 'sale_qty_bl'] = round(table.loc[i, 's_prev_bl_qty'] * table.loc[i, 'sale_qty_pct'],
                                                    2)
        if table.loc[i, 'change_flag'] ==2:
            table.loc[i, 'sale_qty_bl'] = round(table.loc[i - 1, 'sale_qty_bl'] * table.loc[i, 'sale_qty_pct'],
                                                    2)
        if table.loc[i, 'change_flag'] ==3 and table.loc[i-1, 'change_flag'] in [1,2] :
            table.loc[i, 'sale_qty_bl'] = round(table.loc[i-1, 'pf_after_bl_qty'],
                                                    2)
        if table.loc[i, 'change_flag'] ==3 and table.loc[i-1, 'change_flag'] ==3 :
            table.loc[i, 'sale_qty_bl'] = round(table.loc[i - 1, 'sale_qty_bl'] * table.loc[i, 'sale_qty_pct'],
                                                    2)
                                    
    logger.debug(f'{section} - {id} - completed baseline')

    # # produce extended baseline
    # for i in range(0, len(table)):
    #     if table.loc[i, 'change_flag'] == 3:
    #         table.loc[i, 'sale_qty_bl'] = round(table.loc[i - 1, 'sale_qty_bl'] * table.loc[i, 'sale_qty_pct'], 2)
    # logger.debug(f'{sku} - completed pull forward baseline')

        # sku_promo.append(table)

    
    # # define incremental sale
    # table['incremental_qty'] = pd.to_numeric(table['total_sale_qty']) - table['sale_qty_bl']
    
    
    # define final dataframe
    final_df = table[
        ['date', 'uniq_id','sku_root_id', 'promo_id', 'promo_year', 'promo_mechanic', 'discount_depth', 'no_to_pay','no_to_buy', 'change_flag', 'total_sale_qty', 'sale_qty_bl', 'sale_qty_pct']]

    logger.info(f'{section} - {id} - completed baseline and pull forward calculation')
  
    frame.append(final_df)
    
if __name__ == "__main__":
    
    
    start_time = time.time()

    logger.info("Loading input tables from Bigquery....")
    
    logger.info("Loading distinct sections table from Bigquery....")
    section_table = load_section_from_bq(bl_s, project_id)
    
    # Unique sections in category include
    unique_sections = list(section_table["section"].unique())
    logger.info("Unique sections include:")
    for section in unique_sections: logger.info("{a}".format(a=section))
    
    # Loop through sections
    for i_sec in range(0, len(unique_sections)):
        
        section_start_time = time.time()
        section = unique_sections[i_sec]
        
        logger.info("Processing section {a}...".format(a=section))
            
        logger.info("Loading promo table from Bigquery....")
        summary_table = load_promo_from_bq(bl_s, section, project_id)
        summary_table['promo_year'] = summary_table['promo_year'].apply(str)
        summary_table['discount_depth_2'] = summary_table['discount_depth'].fillna('ISNULL')
        summary_table['no_to_pay_2'] = summary_table['no_to_pay'].fillna('ISNULL')
        summary_table['no_to_buy_2'] = summary_table['no_to_buy'].fillna('ISNULL')
        summary_table['uniq_id'] = summary_table['sku_root_id'] + "-"+ summary_table['promo_id'] + "-"+ summary_table['promo_year'] + "-"+ summary_table['promo_mechanic'] + "-"+ summary_table['discount_depth_2']+"-"+ summary_table['no_to_pay_2'].apply(str)+"-"+ summary_table['no_to_buy_2'].apply(str)
        summary_table['total_sale_qty'] = summary_table['total_sale_qty'].apply(pd.to_numeric)
        summary_table['s_prev_bl_qty'] = summary_table['s_prev_bl_qty'].apply(pd.to_numeric)
        summary_table['pf_after_bl_qty'] = summary_table['pf_after_bl_qty'].apply(pd.to_numeric)
        

        logger.info("Loading summary non-promotional transaction table from Bigquery....")
        weekly_agg = load_np_from_bq(bl_s, section, project_id)

        logger.info("Aggregating summary non-promotional transaction table at {a} level".format(a=bl_l))
        agg_np = aggregate_np(weekly_agg, bl_l)
        
        logger.info("Computing no. of unique in-scope ids")
        uniq_id = list(summary_table['uniq_id'].unique())
        logger.info("No. of in-scope skus: {a}".format(a=len(uniq_id)))

        # Compute the % change values in each of the categories used in the baseline
        logger.info("Calculating the % change in baseline values")

        baseline_ref = pd.DataFrame()
        bl_parameter = list(agg_np[bl_l].unique())
        logger.info("No. of in-scope categories used in baseline analyses: {a}".format(a=len(bl_parameter)))

        # Store the baseline results
        baseline_perc_df = pd.DataFrame()
        results_df = pd.DataFrame()

        # Use the multiproc module to process in parallel
        with Manager() as manager:
            frame = manager.list()  # <-- can be shared between processes.
            processes = []

            #Compute the category level baseline metric changes
            for i in range(0, len(bl_parameter), batchsize):

                # Clear the processes list
                processes[:] = []

                start_time_batch = time.time()
                batch = bl_parameter[i:i+batchsize] # the result might be shorter than batchsize at the end
                
                for baseline_parameter in batch:
                    p = Process(target=baseline_pct, args=(frame,baseline_parameter,agg_np, bl_l))  # Passing the list
                    p.start()
                    processes.append(p)
                for p in processes:
                    p.join()
                output = pd.concat(frame)
                baseline_perc_df = pd.concat([baseline_perc_df, output], ignore_index=True, sort =False)
                baseline_perc_df.reset_index(drop=True, inplace=True)
                frame[:] = [] 

                total_time_batch = round((time.time() - start_time_batch), 2)
                logger.debug('Processing category percs with batch size {a} took {b} secs...'.format(a=batchsize, b=total_time_batch))
                logger.info('Category results dataframe has {a} rows and {b} cols...'.format(a=baseline_perc_df.shape[0], b=baseline_perc_df.shape[1]))

            # Compute the SKU level baseline calculations
            for i in range(0, len(uniq_id), batchsize):

                # Clear the processes list
                processes[:] = []

                start_time_batch = time.time()
                batch = uniq_id[i:i+batchsize] # the result might be shorter than batchsize at the end

                for id in batch:
                    p = Process(target=baseline_id, args=(frame,id,summary_table, baseline_perc_df, bl_l, ext_week, section))  # Passing the list
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
        logger.info('Completed baseline processing in {a} mins...'.format(a=total_time))

        # upload the final dataframe onto Bigquery
        logger.info('Uploading baseline table to Bigquery...')
        
        
        if (i_sec == 0):
            pandas_gbq.to_gbq(results_df, 'baseline.baseline', project_id=project_id, if_exists=bl_table_config)
        else:
            pandas_gbq.to_gbq(results_df, 'baseline.baseline', project_id=project_id, if_exists='append')


        logger.info('Completed upload of section baseline to Bigquery...')
        
    #call function to run query in Bigquery to create baseline related tables
    logger.info('Creating baseline tables in Bigquery...')
    baseline_query.baseline_dashboard(project_id, dataset_id)
    logger.info('Completed creating baseline tables in Bigquery...')
    
    total_time = round((time.time() - start_time) / 60, 1)
    logger.info('Completed baseline processing in {a} mins...'.format(a=total_time))
