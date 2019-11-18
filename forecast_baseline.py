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
def forward_looking_baseline_sku(frame, sku: str, summary_table, metrics, forward_period):
    """produce forward looking baseline for the sku
    Args:
        sku(str): sku_root_id
    Returns:
        final_df(pd.DataFrame): Final, preprocessed dataframe
    """
    logger.debug(f'{sku} - being processed')
    
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
            logger.error(f'{sku} - unexpected value in change_flag column')    
    
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
                  columns=['y_t','l_t','b_t','s_t','y_hat_t'],index=df_sku_hist_baseline_sale_amt.index)
    
    df_sku_pred_baseline_sale_amt.append(fit_hist_baseline_sale_amt.forecast(forward_period).rename('y_hat_t').to_frame(), sort=True)
    
     # sale qty
    df_sku_pred_baseline_sale_qty = pd.DataFrame(np.c_[df_sku_hist_baseline_sale_qty, 
                                                       fit_hist_baseline_sale_qty.level, 
                                                       fit_hist_baseline_sale_qty.slope, 
                                                       fit_hist_baseline_sale_qty.season, 
                                                       fit_hist_baseline_sale_qty.fittedvalues],
                  columns=['y_t','l_t','b_t','s_t','y_hat_t'],index=df_sku_hist_baseline_sale_qty.index)
    
    df_sku_pred_baseline_sale_qty.append(fit_hist_baseline_sale_qty.forecast(forward_period).rename('y_hat_t').to_frame(), sort=True)
    
    
    # margin amt
    df_sku_pred_baseline_sale_qty = pd.DataFrame(np.c_[df_sku_hist_baseline_sale_qty, 
                                                       fit_hist_baseline_sale_qty.level, 
                                                       fit_hist_baseline_sale_qty.slope, 
                                                       fit_hist_baseline_sale_qty.season, 
                                                       fit_hist_baseline_sale_qty.fittedvalues],
                  columns=['y_t','l_t','b_t','s_t','y_hat_t'],index=df_sku_hist_baseline_sale_qty.index)
    
    df_sku_pred_baseline_sale_qty.append(fit_hist_baseline_sale_qty.forecast(forward_period).rename('y_hat_t').to_frame(), sort=True)
    
    
    
    
    
    
    
    
    
    
    states1 = pd.DataFrame(np.c_[fit1.level, fit1.slope, fit1.season], columns=['level','slope','seasonal'], index=aust.index)
    states2 = pd.DataFrame(np.c_[fit2.level, fit2.slope, fit2.season], columns=['level','slope','seasonal'], index=aust.index)
    
    
    
    
    
    sku_level = df_sku[bl_l].iloc[0]
    
    # produce baseline dataframe at desired level
    baseline = baseline_ref[baseline_ref[bl_l] == sku_level]

    # merge baseline and sku table
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
    table['incremental_qty'] = pd.to_numeric(table['total_sale_qty']) - table['sale_qty_bl']
    table['incremental_margin'] = pd.to_numeric(table['total_margin_amt']) - table['margin_amt_bl']
    table['ROI_weight'] = table['incremental_sale'] / sum(table['incremental_sale'])

    # define final dataframe
    final_df = table[
        ['date', 'sku_root_id', 'promo_flag_binary', 'change_flag', 'total_sale_amt', 'sale_amt_bl', 'sale_amt_bl_ext',
         'total_sale_qty', 'sale_qty_bl', 'sale_qty_bl_ext', 'total_margin_amt', 'margin_amt_bl', 'margin_amt_bl_ext',
         'incremental_sale', 'incremental_qty', 'incremental_margin', 'ROI_weight', 'sale_amt_promo_flag', 'sale_qty_promo_flag',
         'margin_amt_promo_flag']]

    logger.info(f'{sku} - completed baseline and pull forward calculation')
  
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
