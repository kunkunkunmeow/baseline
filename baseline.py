from multiprocessing import Process, Manager
import pandas as pd
import pandas_gbq
import numpy as np
from tqdm import tqdm
import time
import logging

# Loading data from Bigquery

start_time = time.time()
project_id = "gum-eroski-dev"

logger = logging.getLogger('baseline_calculation')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('baseline.log')
fh.setLevel(logging.DEBUG)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)


# logger.basicConfig( filename = 'baseline/', filemode = 'a', format='%(asctime)s %(levelname)-8s %(message)s', level=logger.INFO, datefmt='%Y-%m-%d %H:%M:%S')

def load_t1_from_bq():
    summary_sql = """
    SELECT date, sku_root_id , area, section, category, subcategory , segment , total_sale_amt, total_sale_qty , total_margin_amt , promo_flag_binary, sale_amt_promo_flag, sale_qty_promo_flag, margin_amt_promo_flag
    FROM `ETL.aggregate_weekly_transaction_summary`
    WHERE category = "CONFECCION MUJER"   """
    start = time.time()
    for i in tqdm(range(1), desc= 'loading summary table from bigquery'):
        summary_table = pandas_gbq.read_gbq(summary_sql, project_id = project_id)
    # print('loading weekly summary data from bigquery took', round((time.time()-start)/60,1), 'minutes.')
    #FROM `WIP.summary_weekly_test` 
    logger.info("Loaded summary table from bigquery")

    return summary_table

def load_t2_from_bq():
    weeklyagg_sql = """
    SELECT date, sku_root_id, area, section, category, subcategory, segment, sum(total_sale_amt) as sale_amt_np, sum(total_sale_qty) as sale_qty_np, sum(total_margin_amt) as margin_amt_np 
    FROM `ETL.aggregate_weekly_transaction_to_sku`    
    WHERE promo_flag = false
    AND category = "CONFECCION MUJER"
    group by date, sku_root_id, area, section, category, subcategory, segment """
    start = time.time()
    for i in tqdm(range(1), desc= 'loading weekly transaction table from bigquery'):
        weekly_agg = pandas_gbq.read_gbq(weeklyagg_sql, project_id = project_id)
    print('loading weekly aggregated data from bigquery took', round((time.time()-start)/60,1), 'minutes.')
    # FROM `WIP.agg_weekly_test`
    logger.info("Loaded weekly_agg table from bigquery")

    return weekly_agg

# define key parameters 
bl_l = "section"
ext_day = 1
metrics = {
    'sale_amt':['sale_amt_np', 'total_sale_amt'],
    'sale_qty':['sale_qty_np', 'total_sale_qty'],
    'margin_amt':['margin_amt_np','total_margin_amt']
}


# define function to aggregate table by defined level
def aggregate_np (weekly_agg, level):
    agg = weekly_agg.groupby(["date", level], as_index = False)['sale_amt_np','sale_qty_np','margin_amt_np'].sum()
    logger.info("aggregate_np table created")
    return agg



# define function to process baseline for one sku 
def baseline_sku(frame, sku:str, summary_table, agg_np):
    """produce baseline and cannibalisation line for the sku

    Args:
        sku(str): sku_root_id
    Returns:
        final_df(pd.DataFrame): Final, preprocessed dataframe
    """
    
    # get dataframe for the specific sku
    df_sku = summary_table[summary_table.sku_root_id == sku].sort_values(by = ['date']).reset_index(drop = True)
    logger.info(f'{sku} - sku table created')
    

    # define function to aggregate the sku into desired level and get change % over date
    def aggregate_sku (agg_np, level:str):
        """produce aggregated results of desired level and the change % of total sale quantity along the timeline

        Args:
            level(str): desired level for the sku to be grouped to 
        Returns:
            agg_np_sku(pd.DataFrame): Final, preprocessed dataframe
        """
        # locate the group that needs to be aggregated
        sku_level = summary_table.loc[summary_table['sku_root_id'] == sku, level].iloc[0]
        
        # get the aggregated none promotion data for the group that the SKU belongs to
        agg_np_sku = agg_np[agg_np[level] == sku_level].sort_values(by = ['date']).reset_index(drop = True)
        for metric in metrics.keys():
            agg_np_sku[f'{metric}_pct'] = 1
            for i in range(1,len(agg_np_sku)):
                if agg_np_sku.loc[i-1,metrics[metric][0]] == 0:
                    agg_np_sku.loc[i,f'{metric}_pct'] = np.nan
                else:
                    agg_np_sku.loc[i,f'{metric}_pct'] = float(agg_np_sku.loc[i,metrics[metric][0]]/agg_np_sku.loc[i-1,metrics[metric][0]])
        return agg_np_sku
    
    # produce baseline dataframe at desired level 
    baseline = aggregate_sku(agg_np, bl_l)
    logger.info(f'{sku} - baseline table created')

    # merge baseline, cannibalisation and sku table
    table = pd.merge(df_sku[['date', 'sku_root_id','promo_flag_binary','total_sale_amt','total_sale_qty','total_margin_amt','sale_amt_promo_flag', 'sale_qty_promo_flag','margin_amt_promo_flag']], 
            baseline[['date',bl_l,'sale_amt_pct', 'sale_qty_pct', 'margin_amt_pct']], on=['date']).reset_index(drop=True)
    logger.info(f'{sku} - merged table created')

    # define change_flag to be the reference of baseline and cannibalisation calculation
    # set 1 : for the first day on promotion
    # set 2: for the rest days on promotion after first day
    # set 3: for the extension days of baseline calculation
    table['change_flag'] = table['promo_flag_binary']
    for i in range(1,len(table)):
        if table.loc[i, 'change_flag'] == 1:
            table.loc[i, 'change_flag'] =2
    for i in range(0,len(table)-1):
        if table.loc[i, 'change_flag'] == 0 and table.loc[i+1, 'change_flag'] == 2:
            table.loc[i+1,'change_flag'] =1
    for i in range(0,len(table)-ext_day):
        for j in range(1, ext_day+1):
            if table.loc[i, 'change_flag'] ==2 and table.loc[i+j, 'change_flag'] == 0:
                table.loc[i+j, 'change_flag'] =3
    logger.info(f'{sku} - change flag done')

    #produce baseline
    for metric in metrics.keys():
        table.loc[0,f'{metric}_bl'] = np.nan
        for i in range(1,len(table)):
            if table.loc[i, 'change_flag'] == 0:
                table.loc[i,f'{metric}_bl'] = np.nan
            if table.loc[i, 'change_flag'] == 1 and table.loc[i-1, 'change_flag'] in [0,3]: 
                table.loc[i-1,f'{metric}_bl'] = float(table.loc[i-1,metrics[metric][1]])
            if table.loc[i, 'change_flag'] in [1,2]:
                table.loc[i,f'{metric}_bl'] = round(table.loc[i-1,f'{metric}_bl'] * table.loc[i,f'{metric}_pct'],2)
    logger.info(f'{sku} - baseline done')

    #produce extended baseline
    for metric in metrics.keys():
        table.loc[0,f'{metric}_bl_ext'] = np.nan
        for i in range(1,len(table)):
            if table.loc[i, 'change_flag'] in [0,1]:
                table.loc[i,f'{metric}_bl_ext'] = np.nan
            if table.loc[i, 'change_flag'] ==3 and table.loc[i-1, 'change_flag'] ==2:
                table.loc[i-1,f'{metric}_bl_ext'] = table.loc[i-1,f'{metric}_bl']
            if table.loc[i, 'change_flag'] ==3:
                table.loc[i,f'{metric}_bl_ext'] = round(table.loc[i-1,f'{metric}_bl'] * table.loc[i,f'{metric}_pct'],2)
    logger.info(f'{sku} - extended baseline table created')
    
    #defiine incremental sale
    table['incremental_sale'] = pd.to_numeric(table['total_sale_amt']) - table['sale_amt_bl']
    table['incremental_margin'] = pd.to_numeric(table['total_margin_amt']) - table['margin_amt_bl']
    table['ROI_weight'] = table['incremental_sale']/sum(table['incremental_sale'])
    
    # define final dataframe 
    final_df = table[['date', 'sku_root_id', 'promo_flag_binary', 'change_flag', 'total_sale_amt', 'sale_amt_bl', 'sale_amt_bl_ext','total_sale_qty', 'sale_qty_bl', 'sale_qty_bl_ext', 'total_margin_amt', 'margin_amt_bl', 'margin_amt_bl_ext' ,'incremental_sale','incremental_margin','ROI_weight','sale_amt_promo_flag','sale_qty_promo_flag', 'margin_amt_promo_flag']]
    
    logger.info(f'{sku} - final table created')

    frame.append(final_df)


#get unique sku list


#for each sku, process the baseline and cannibalisation then concat into final dataframe 
# frame = []
# for sku in tqdm(range(len(uniq_sku)), desc="processing baseline for each sku"):
#     df_bl = baseline_sku(uniq_sku[sku])
#     frame.append(df_bl)

if __name__ == "__main__":
    start_time = time.time()
    
    logger.info("start of the script")

    summary_table = load_t1_from_bq()

    weekly_agg = load_t2_from_bq()

    agg_np = aggregate_np(weekly_agg, bl_l)

    uniq_sku = list(summary_table['sku_root_id'].unique())

    with Manager() as manager:
        frame = manager.list()  # <-- can be shared between processes.
        processes = []
        for i in uniq_sku:
            p = Process(target=baseline_sku, args=(frame,i,summary_table, agg_np))  # Passing the list
            p.start()
            processes.append(p)
        for p in processes:
            p.join()
        final_df = pd.concat(frame)
        final_df.reset_index(drop=True, inplace=True)

        for i in list(final_df.columns)[2:]:
            final_df[i] = pd.to_numeric(final_df[i])

        final_df = final_df.where((pd.notnull(final_df)), None)
        logger.info('finish baseline processing')

        # upload the final dataframe onto '{sku} - final taBigquery
        pandas_gbq.to_gbq(final_df,'WIP.bl_multi2_1113', project_id = project_id, if_exists='replace')
        logger.info('finished uploading onto Bigquery')
        print('the whole process took', round((time.time()-start_time)/60,1), 'minutes.')

