import pandas as pd
from multiprocessing import Process, Manager
import pandas_gbq
import numpy as np
from sklearn import linear_model
from tqdm import tqdm
import time
import logging
from datetime import timedelta  
import baseline_query

# Input global variables 
# Reformat code to accept these variables as input

# linear regression variables
max_limit = 2
min_limit = 1/max_limit
min_points = 2

# thresholds
store_threshold = 0.1
R2_threshold = 0.6
max_price_change = 0.3

# Project ID
project_id = "gum-eroski-dev"
dataset_id = "price_elast"

# Define key baseline parameters
# Category level used to compute the baseline
# Need to be section level or below (i.e category, subcategory, etc)
bl_l = "section"

# Scope for the baseline (at an area level)
bl_s = "ALIMENTACION"

# Category metadata
#category_df = pd.read_csv('category_metadata.csv')
category ="""LECHE
GALLETAS
"""


"""PESCADO Y MARISCO CONGELADO
LECHE
YOGURES Y POSTRES
QUESOS
HUEVOS FRESCOS
PATATAS FRITAS Y SNACKS
TOMATE
LEGUMBRES
AZUCAR Y EDULCORANTES
GALLETAS
PAN DE MOLDE
AGUAS
BEBIDAS REFRESCANTES
LIMPIADORES
ROLLOS DE COCINA Y SERVILLETAS
HIGIENE BUCAL
PARAFARMACIA
COMPLEMENTOS BASICOS
VITRINA
FRUTA
LISTO PARA CONSUMIR
HORTALIZA
GRANELES
IV GAMA
QUESOS MOSTRADOR
CALENTAR Y LISTO
CARNICOS MOSTRADOR
PATES MOSTRADOR
CARNICOS PIEZA
POLLO ENVASADO AVES
VACUNO ENVASADO
PORCINO ENVASADO
ELABORADOS ENVASADO
MOSTRADOR PESCADERIA
COCIDOS PORCION
PESCADO FRESCO ENVASADO
ESPECIALIDADES
BOLLERIA
SALCHICHAS
DISFRUTA COCINANDO
JAMONES PIEZA
PAVO Y OTRAS AVES ENVASADO
ECOLOGICO
LISTO PARA COMER
PAN
CURADOS, EMBUT. E IBERICOS
PASTELERIA
CORDERO ENVASADO
CONEJO ENVASADO
MATERIAS PRIMAS Y MASAS CONGELADAS
QUESOS RECIEN CORTADOS
STAND IBERICOS
COCINA IN SITU"""

#QUESOS PORCIÓN
#PATÉS PORCIÓN

category = ["\'"+each+"\'" for each in list(category.split("\n"))]

# Append or replace destination table (either 'append' or 'replace')
bl_table_config = 'replace'

# Set batch size
batchsize = 50

# Set logger properties
logger = logging.getLogger('price_elasticity_calculation')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('price_elasticity.log')
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

def load_daily_trans_from_bq(cat, project_id):
    start_time = time.time()
    
    sql_str = """
    SELECT
        sku_root_id,
        store_id, std_price_per_unit,
        AVG(avg_sales_qty_per_week) as avg_sales_qty,
        AVG(actual_price) as actual_price,
        COUNT(week_start_date) as duration_weeks,
        STDDEV(avg_sales_qty_per_week) as std_dev_sales_qty FROM (with temp as (
            SELECT
                sku_root_id,
                store_id,
                DATE_TRUNC(date, WEEK(MONDAY)) as week_start_date,
                std_price_per_unit, SUM(total_sale_qty) as avg_sales_qty_per_week,
                SAFE_DIVIDE(SUM(total_sale_amt),
                SUM(total_sale_qty)) as actual_price
            FROM `gum-eroski-dev.ETL.aggregate_daily_transaction_to_sku`
                WHERE area in ("ALIMENTACION", "FRESCOS")
                AND category = {c}
                AND promo_flag = false
                AND store_id in ('149','155','157','159','164','165','182','184','185','190','192','201','207','208','209','212','213','5','6','16','22','25','26','28','29','30','31','36','41','46','47','51','52','68','74','86','87','88','96','98','99','101','103','106','108','119','120','125','138','143','144','263','264','266','280','281','282','283','290','300','302','308','316','320','323','326','330','217','219','223','224','230','231','233','234','235','236','238','240','243','245','248','249','259','393','397','400','401','403','410','418','419','420','422','424','427','429','430','475','331','334','335','346','352','357','358','359','371','378','379','380','381','383','385','387','390','476','479','480','484','488','489','495','496','498','501','502','505','510','512','544','546','547','548','549','550','551','552','553','555','556','558','562','587','599','602','607','727','730','733','734','735','736','748','890','996','1000','1362','1373','1374','1392','1393','1499','1501','1504','2003','2005','3001','608','663','664','665','666','668','669','671','673','674','677','678','679','690','691','718','719','720','721','723','726','3057','3058','3059','3090','3091','3092','3095','3097','3098','3100','3102','3104','3105','3106','3107','3108','3109','3110','3111','3112','3113','3114','3116','3004','3008','3011','3013','3017','3020','3023','3024','3025','3026','3027','3028','3029','3034','3036','3042','3045','3049','3052','3053','3054','3055','3056','3177','3183','3188','3189','3194','3196','3203','3204','3206','3207','3208','3209','3213','3214','3219','3224','3226','3228','3229','3230','3232','3235','3238','3117','3118','3119','3120','3121','3122','3123','3125','3126','3127','3128','3129','3130','3131','3138','3139','3154','3155','3156','3162','3163','3165','3175','3239','3240','3242','3244','3245','3247','3249','3250','3252','3254','3256','3257','3259','3261','3262','3263','3264','3266','3268','3294','3295','3297','3298','3299','3368','3369','3370','3371','3372','3373','3382','3384','3387','3643','3978','3979','3981','3982','3984','3986','3987','3988','3989','3991','3992','3994','3995','3644','3646','3647','3648','3885','3886','3888','3902','3906','3907','3908','3911','3912','3913','3914','3917','3919','3922','3971','3972','3975','3976','3977','4264','4273','4277','4296','4297','4299','4357','4360','4361','4041','4047','4090','4091','4102','4103','4106','4111','4128','4134','4203','4247','4261','4484','4600','4369','4371','4373','4374','4382','4384','4388','4390','4469','4705','4749','4750','4751','4752','4753','4754','4755','4756','4757','4758','4759','4761','4763','4764','4767','4768','4785','4786','4935','4937','5382','6413','6414','6438','6483','7514','7564','7565','7566','7567','7569','7573','8122','8133','8143','8144','8149','8206','8212','8216','8219','8221','9050','9059','9064','6767','6768','9891','271','288','262','3985','3990','4395','6136','6282','6283','6284','7575','8121','8127','8135','8211','9026','9030','9061','9706','9803','9877','9879','9887','9889','9959','210','433','5007','5091','5106','5111','5301','5318','5725','5744','7444','5016','5086','5371','175','187','202','250','399','445','5009','5021','5040','5052','5083','5908','7423')
                AND total_sale_qty <> 0
                GROUP BY sku_root_id,
                    week_start_date,
                    store_id,
                    std_price_per_unit
                )
    SELECT * from temp
    WHERE EXTRACT(MONTH from week_start_date) <> 12
    AND SAFE_DIVIDE(ABS(actual_price-std_price_per_unit),std_price_per_unit)<0.1)
    GROUP BY sku_root_id,
        store_id,
        std_price_per_unit
    """.format(c=cat)
    start = time.time()
    
    for i in tqdm(range(1), desc='Loading table...'):
        category_table = pandas_gbq.read_gbq(sql_str, project_id=project_id)
    
    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of category table from Bigquery {a} mins...".format(a=total_time))
    
    return category_table

def unit_cost_table(category, project_id):
    sql_str = """
    SELECT DISTINCT price.sku_root_id as sku_root_id, price.cost_per_unit as cost_per_unit, cat.category as category
    FROM `gum-eroski-dev.ETL.aggregate_std_price_margin` as price
    JOIN `gum-eroski-dev.ETL.sku` as cat
    ON price.sku_root_id = cat.sku_root_id
    WHERE cat.category = {c}
    """.format(c=category)
    start = time.time()
    
    for i in tqdm(range(1), desc='Loading table...'):
        cost_per_unit_table = pandas_gbq.read_gbq(sql_str, project_id=project_id)
    
    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of category table from Bigquery {a} mins...".format(a=total_time))
    
    return cost_per_unit_table

def linear_reg(frame, agg_np, cost_per_unit_table, sku, max_limit, min_limit, min_points):        
    # get the aggregated none promotion data for the group that the SKU belongs to
 
    #fullData = pd.DataFrame()
    
    logger.info(f'{sku} - being processed...')
    
    #if sku == "10296796": logger.info(agg_np)
        
    # set dataframe for each sku
    fullData = agg_np.loc[agg_np['sku_root_id']==sku]
    #df.loc[df['column_name'] == some_value]
    
    #if sku == "10296796": logger.info(fullData)
    
    # get store ids
    store_ids = fullData.store_id.unique()
    #if sku == "10296796":
    #    logger.info("number of stores= {z}".format(z=len(store_ids)))
    #    logger.info(store_ids)
    
    # initialise output lists
    sku_id = []
    store = []
    coeficient = []
    gradient = []
    R2 = []
    c = []
    norm_factor = []
    points = []
    average_price = []
    cost_per_unit = []
    
    cost = float(cost_per_unit_table.loc[cost_per_unit_table['sku_root_id']==sku]['cost_per_unit'].unique())
    
    for store_id in store_ids:
        data = fullData.loc[fullData['store_id']==store_id]
        Nfactor = data.mean(axis=0)['avg_sales_qty']
        avg_price = data.mean(axis=0)['actual_price']
        
        #if sku == "10296796":
        #    logger.info(counting)
        #    counting+=data.shape[0]
        
        feat = data[['std_price_per_unit']]
        qty = data[['avg_sales_qty']]
        
        X = feat
        y = qty
        lm = linear_model.LinearRegression()
        model = lm.fit(X,y)
        
        predictions = lm.predict(y)
        
        store.append(store_id)
        coeficient.append(lm.coef_[0][0])
        R2.append(lm.score(X,y))
        points.append(data.shape[0])
        c.append(lm.intercept_[0])
        gradient.append(lm.coef_[0][0]/Nfactor)
        norm_factor.append(Nfactor)
        sku_id.append(sku)
        average_price.append(avg_price)
        cost_per_unit.append(cost)
        
    list_of_tuples1 = list(zip(sku_id, store, coeficient, gradient, R2, c, norm_factor, points, average_price, cost_per_unit))
    df = pd.DataFrame(list_of_tuples1, columns = ['sku', 'store', 'coeficient', 'gradient_Nfactor_applied', 'R2', 'intercept', 'Nfactor', 'points', 'avg_price', 'cost_per_unit'])
    
    # drop rows where points less than or equal to min_points
    df.drop(df[(df['points']<= min_points)].index, inplace=True)
    
    # select all negative rows
    neg_all = df
    # drop any rows with positive gradients
    indexNames = neg_all[(neg_all['gradient_Nfactor_applied']>=0)].index
    neg_all.drop(indexNames , inplace=True)
    
    # get list of stores
    store_selection = neg_all.store.unique()
    # select df using store_selection
    store_selection_df = fullData.loc[fullData['store_id'].isin(store_selection)]
    
    if sku == "89961": logger.info(store_selection_df.head())
    
    def aggregate_group(group):
        # calculates avg gradient, avg R2, optimal price, % price change for group
        grad = group.mean(axis=0)['gradient_Nfactor_applied']
        R2 = group.mean(axis=0)['R2']
        opt_price = (1-grad*(group.mean(axis=0)['avg_price']+group.mean(axis=0)['cost_per_unit']))/(-2*grad)
        price_change = (opt_price/group.mean(axis=0)['avg_price'])-1
        avg_Nfactor = group.mean(axis=0)['Nfactor']
        avg_price = group.mean(axis=0)['avg_price']
        
        return grad, R2, opt_price, price_change, avg_Nfactor, avg_price


if __name__ == "__main__":
    
    start_time = time.time()

    logger.info("Loading input tables from Bigquery....")
    
    # loop through each category
    for i_sec, each in enumerate(category):
        
        section_time = time.time()
        
        logger.info("Processing category {a}...".format(a=each))
        
        category_table = load_daily_trans_from_bq(each, project_id)
        
        cost_per_unit_table = unit_cost_table(each, project_id)
        
        skus = list(category_table['sku_root_id'].unique())
        
        results_df = pd.DataFrame()
        
        with Manager() as manager:
            frame = manager.list()  # <-- can be shared between processes.
            processes = []
            
            for i in range(0, len(skus), batchsize):

                # Clear the processes list
                processes[:] = []

                start_time_batch = time.time()
                batch = skus[i:i+batchsize] # the result might be shorter than batchsize at the end

                for product in batch:
                    p = Process(target=linear_reg, args=(frame, category_table, cost_per_unit_table, product, max_limit, min_limit, min_points))  # Passing the list
                    p.start()
                    processes.append(p)
                for p in processes:
                    p.join()
                if len(frame) != 0:
                    output = pd.concat(frame)
                    results_df = pd.concat([results_df, output], ignore_index=True, sort =False)
                    results_df.reset_index(drop=True, inplace=True)
                frame[:] = []

                total_time_batch = round((time.time() - start_time_batch), 2)
                #logger.debug('Processing with batch size {a} took {b} secs...'.format(a=batchsize, b=total_time_batch))

                logger.info('Results dataframe has {a} rows and {b} cols...'.format(a=results_df.shape[0], b=results_df.shape[1]))

            # Convert all nulls to None
            results_df = results_df.where((pd.notnull(results_df)), None)


            #total_time = round((time.time() - section_start_time) / 60, 1)
            #logger.info('Completed baseline processing in {a} mins...'.format(a=total_time))

            # upload the final dataframe onto Bigquery
            logger.info('Uploading baseline table to Bigquery...')
            
            logger.info(i_sec)
            pandas_gbq.to_gbq(results_df, 'price_elast.lin_reg_sku_level_{c}'.format(c=each.replace("\'","").replace(".","").replace(" ","").replace(",","")), project_id=project_id, if_exists=bl_table_config)
            """
            if (i_sec == 0):
                pandas_gbq.to_gbq(results_df, 'price_elast.lin_reg_store_level_{c}'.format(c=each.replace("\'","").replace(".","").replace(" ","").replace(",","")), project_id=project_id, if_exists=bl_table_config)
            else:
                pandas_gbq.to_gbq(results_df, 'price_elast.lin_reg_store_level_{c}'.format(c=each.replace("\'","").replace(".","").replace(" ","").replace(",","")), project_id=project_id, if_exists='append')
            """

            logger.info('Completed upload of section baseline to Bigquery...')

    #     #call function to run query in Bigquery to create baseline related tables
    #     logger.info('Creating baseline tables in Bigquery...')
    #     baseline_query.baseline_dashboard(project_id, dataset_id)
    #     logger.info('Completed creating baseline tables in Bigquery...')

        #total_time = round((time.time() - section_time) / 60, 1)
        #logger.info('Completed baseline processing in {a} mins...'.format(a=total_time))
