import pandas as pd
from multiprocessing import Process, Manager
import pandas_gbq
import numpy as np
from statsmodels.tsa.api import ExponentialSmoothing, SimpleExpSmoothing, Holt
from tqdm import tqdm
import time
import logging

# Input global variables 
# Reformat code to accept these variables as input

# Project ID
project_id = "gum-eroski-dev"

# Define key forward baseline parameters

# Scope for the baseline (at an area level)
bl_s = "ALIMENTACION"

# Append or replace destination table (either 'append' or 'replace')
bl_table_config = 'replace'

# Forward looking period (wks)
forward_period = 12

# Baseline % metrics
metrics = {
    'sale_amt': ['sale_amt_np', 'total_sale_amt'],
    'sale_qty': ['sale_qty_np', 'total_sale_qty'],
    'margin_amt': ['margin_amt_np', 'total_margin_amt']
}
# Set batch size
batchsize = 50

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

# Function to load distinct historic baseline section data at a sku level from bigquery
def load_t0_from_bq(area, project_id):
    start_time = time.time()

    summary_sql = """
    SELECT distinct sku.section 
    FROM `gum-eroski-dev.baseline_performance.baseline` bline
    INNER JOIN `gum-eroski-dev.ETL.root_sku` sku
    ON sku.sku_root_id = bline.sku_root_id
    WHERE sku.area = "%s"   """ %(area)
    start = time.time()

    for i in tqdm(range(1), desc='Loading table...'):
        section_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of distinct baseline sections from Bigquery in {a} mins...".format(a=total_time))

    return section_table


# Function to load baseline summary data at a sku level from bigquery
def load_t1_from_bq(section, project_id):
    start_time = time.time()

    summary_sql = """
    SELECT bline.*
    FROM `gum-eroski-dev.baseline_performance.baseline` bline
    INNER JOIN `gum-eroski-dev.ETL.root_sku` sku
    ON sku.sku_root_id = bline.sku_root_id
    WHERE sku.section = "%s"   """ %(section)
    start = time.time()

    for i in tqdm(range(1), desc='Loading table...'):
        summary_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of baseline summary table from Bigquery in {a} mins...".format(a=total_time))

    return summary_table


# define function to process baseline for one sku
def forward_looking_baseline_sku(sku_pred_frame, sku_metric_frame, sku: str, summary_table, metrics, forward_period):
    """produce forward looking baseline for the sku
    Args:
        sku(str): sku_root_id
    Returns:
        final_df(pd.DataFrame): Final, preprocessed dataframe
    """
    logger.debug(f'{sku} - being processed...')
    
    # get dataframe for the specific sku
    df_sku = summary_table[summary_table.sku_root_id == sku].sort_values(by=['date']).reset_index(drop=True)
    
    # get the input data for the training model
    def fw_baseline_input(sku, change_flag, actual, baseline, ext):
        if change_flag == 0:
            return actual
        elif change_flag in (1,2):
            return baseline
        elif change_flag == 3:
            return ext
        else:
            logger.error(f'{sku} - unexpected value in change_flag column!')    
    
    df_sku['hist_baseline_sale_amt'] = df_sku.apply(lambda x: fw_baseline_input(x['sku_root_id'], 
                                                                       x['change_flag'], 
                                                                       x['total_sale_amt'], 
                                                                       x['sale_amt_bl'], 
                                                                       x['sale_amt_bl_ext']),axis=1)
    
    
    df_sku['hist_baseline_sale_qty'] = df_sku.apply(lambda x: fw_baseline_input(x['sku_root_id'], 
                                                                       x['change_flag'], 
                                                                       x['total_sale_qty'], 
                                                                       x['sale_qty_bl'], 
                                                                       x['sale_qty_bl_ext']),axis=1)
    
    
    df_sku['hist_baseline_margin_amt'] = df_sku.apply(lambda x: fw_baseline_input(x['sku_root_id'], 
                                                                       x['change_flag'], 
                                                                       x['total_margin_amt'], 
                                                                       x['margin_amt_bl'], 
                                                                       x['margin_amt_bl_ext']),axis=1)
    
    logger.debug(f'{sku} - completed calculation of historic baseline values...')
    
    # Split dataframe to 3 time series
    df_sku_hist_baseline_sale_amt = df_sku[['date','hist_baseline_sale_amt']]
    df_sku_hist_baseline_sale_amt = df_sku_hist_baseline_sale_amt.set_index('date', drop=True)
    
    df_sku_hist_baseline_sale_qty = df_sku[['date','hist_baseline_sale_qty']]
    df_sku_hist_baseline_sale_qty = df_sku_hist_baseline_sale_qty.set_index('date', drop=True)
    
    df_sku_hist_baseline_margin_amt = df_sku[['date','hist_baseline_margin_amt']]
    df_sku_hist_baseline_margin_amt = df_sku_hist_baseline_margin_amt.set_index('date', drop=True)
    
    # Split to test and train datasets
    # TODO
    
    # Use Holt Winters multiplicative exponential smoothing method
    fit_hist_baseline_sale_amt = ExponentialSmoothing(df_sku_hist_baseline_sale_amt, seasonal_periods=52, trend='add', 
                                                      seasonal='mul', damped=True).fit(use_boxcox=True)
    
    fit_hist_baseline_sale_qty = ExponentialSmoothing(df_sku_hist_baseline_sale_qty, seasonal_periods=52, trend='add', 
                                                      seasonal='mul', damped=True).fit(use_boxcox=True)
    
    fit_hist_baseline_margin_amt = ExponentialSmoothing(df_sku_hist_baseline_margin_amt, seasonal_periods=52, trend='add', 
                                                      seasonal='mul', damped=True).fit(use_boxcox=True)
    
    logger.debug(f'{sku} - completed fit of exponential smoothing model...')
    
    results_df = pd.DataFrame(columns=['sku_root_id', 'alpha','beta','phi','gamme','l_0','b_0','SSE'])
    params = ['smoothing_level', 'smoothing_slope', 'damping_slope', 'smoothing_seasonal', 'initial_level', 'initial_slope']
    param_list = [sku]
    for p in params:
        param_list.append(fit_hist_baseline_sale_amt.params[p])
    
    # add SSE
    param_list.append(fit_hist_baseline_sale_amt.sse)
    
    # add to sku metrics dataframe 
    metrics_series = pd.Series(param_list, index=results_df.columns)
    results_df = results_df.append(metrics_series, ignore_index=True)
    
    # forecast x periods and view internals of the smoothing model
    # sale amt
    df_sku_pred_baseline_sale_amt = pd.DataFrame(np.c_[df_sku_hist_baseline_sale_amt, 
                                                       fit_hist_baseline_sale_amt.level, 
                                                       fit_hist_baseline_sale_amt.slope, 
                                                       fit_hist_baseline_sale_amt.season, 
                                                       fit_hist_baseline_sale_amt.fittedvalues],
                  columns=['y_t_sale_amt','l_t_sale_amt','b_t_sale_amt','s_t_sale_amt','y_hat_t_sale_amt'],index=df_sku_hist_baseline_sale_amt.index)
    
    df_sku_pred_baseline_sale_amt.append(fit_hist_baseline_sale_amt.forecast(forward_period).rename('y_hat_t_sale_amt').to_frame(), sort=True)
    
     # sale qty
    df_sku_pred_baseline_sale_qty = pd.DataFrame(np.c_[df_sku_hist_baseline_sale_qty, 
                                                       fit_hist_baseline_sale_qty.level, 
                                                       fit_hist_baseline_sale_qty.slope, 
                                                       fit_hist_baseline_sale_qty.season, 
                                                       fit_hist_baseline_sale_qty.fittedvalues],
                  columns=['y_t_sale_qty','l_t_sale_qty','b_t_sale_qty','s_t_sale_qty','y_hat_t_sale_qty'],index=df_sku_hist_baseline_sale_qty.index)
    
    df_sku_pred_baseline_sale_qty.append(fit_hist_baseline_sale_qty.forecast(forward_period).rename('y_hat_t_sale_qty').to_frame(), sort=True)
    
    
    # margin amt
    df_sku_pred_baseline_margin_amt = pd.DataFrame(np.c_[df_sku_hist_baseline_margin_amt, 
                                                       fit_hist_baseline_margin_amt.level, 
                                                       fit_hist_baseline_margin_amt.slope, 
                                                       fit_hist_baseline_margin_amt.season, 
                                                       fit_hist_baseline_margin_amt.fittedvalues],
                  columns=['y_t_margin_amt','l_t_margin_amt','b_t_margin_amt','s_t_margin_amt','y_hat_t_margin_amt'],index=df_sku_hist_baseline_margin_amt.index)
    
    df_sku_pred_baseline_margin_amt.append(fit_hist_baseline_margin_amt.forecast(forward_period).rename('y_hat_t_margin_amt').to_frame(), sort=True)
    
    logger.debug(f'{sku} - completed prediction of exponential smoothing model...')
    
    # obtain strength of seasonal and trend components
    # TODO
    
    logger.info(f'{sku} - completed forecast baseline calculation')
    
    frame.append(final_df)
    
if __name__ == "__main__":
    
    
    start_time = time.time()

    logger.info("Loading input tables from Bigquery....")
    
    logger.info("Loading distinct sections table from Bigquery....")
    section_table = load_t0_from_bq(bl_s, project_id)
    
    # Unique sections in category include
    unique_sections = list(section_table["section"].unique())
    logger.info("Unique sections include:")
    for section in unique_sections: logger.info("{a}".format(a=section))
    
    # Loop through sections
    for i_sec in range(0, len(unique_sections)):
        
        section_start_time = time.time()
        section = unique_sections[i_sec]
        
        logger.info("Processing section {a}...".format(a=section))
        
        # Compute the baseline for each section     
        logger.info("Loading summary transaction table from Bigquery....")
        summary_table = load_t1_from_bq(section, project_id)

        logger.info("Loading summary non-promotional transaction table from Bigquery....")
        weekly_agg = load_t2_from_bq(section, project_id)

        logger.info("Aggregating summary non-promotional transaction table at {a} level".format(a=bl_l))
        agg_np = aggregate_np(weekly_agg, bl_l)

        logger.info("Computing no. of unique in-scope skus")
        uniq_sku = list(summary_table['sku_root_id'].unique())
        logger.info("No. of in-scope skus: {a}".format(a=len(uniq_sku)))

        # Compute the % change values in each of the categories used in the baseline
        logger.info("Calculating the % change in baseline values")

        baseline_ref = pd.DataFrame()
        bl_parameter = list(agg_np[bl_l].unique())
        logger.info("No. of in-scope categories used in baseline analyses: {a}".format(a=len(bl_parameter)))

        # Store the baseline results
        results_df = pd.DataFrame()

        # Use the multiproc module to process in parallel
        with Manager() as manager:
            frame = manager.list()  # <-- can be shared between processes.
            processes = []

            # Compute the SKU level baseline calculations
            for i in range(0, len(uniq_sku), batchsize):

                # Clear the processes list
                processes[:] = []

                start_time_batch = time.time()
                batch = uniq_sku[i:i+batchsize] # the result might be shorter than batchsize at the end

                for sku in batch:
                    p = Process(target=baseline_sku, args=(frame,sku,summary_table, baseline_perc_df, bl_l, metrics, ext_day))  # Passing the list
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

        for i in list(results_df.columns)[2:]:
            results_df[i] = pd.to_numeric(results_df[i])

        # Convert all nulls to None
        results_df = results_df.where((pd.notnull(results_df)), None)


        total_time = round((time.time() - section_start_time) / 60, 1)
        logger.info('Completed baseline processing in {a} mins...'.format(a=total_time))

        # upload the final dataframe onto Bigquery
        logger.info('Uploading baseline table to Bigquery...')
        
        if (i_sec == 0):
            pandas_gbq.to_gbq(results_df, 'baseline_performance.baseline', project_id=project_id, if_exists=bl_table_config)
        else:
            pandas_gbq.to_gbq(results_df, 'baseline_performance.baseline', project_id=project_id, if_exists='append')


        logger.info('Completed upload of section baseline to Bigquery...')
    
    total_time = round((time.time() - start_time) / 60, 1)
    logger.info('Completed baseline processing in {a} mins...'.format(a=total_time))
