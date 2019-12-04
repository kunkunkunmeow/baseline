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

# Project ID
project_id = "gum-eroski-dev"
dataset_id = "baseline"

# Define key baseline parameters
# Category level used to compute the baseline
# Need to be section level or below (i.e category, subcategory, etc)
bl_l = "section"

# Scope for the baseline (at an area level)
bl_s = "ALIMENTACION"

# Category scope
category = """PESCADO Y MARISCO CONGELADO
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
QUESOS PORCIÓN
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
PATÉS PORCIÓN
PAN
CURADOS, EMBUT. E IBERICOS
PASTELERIA
CORDERO ENVASADO
CONEJO ENVASADO
MATERIAS PRIMAS Y MASAS CONGELADAS
QUESOS RECIEN CORTADOS
STAND IBERICOS
COCINA IN SITU"""

category="\'"+category.replace("\n","\',\'")+"\'"
category = list(category.split(","))

# Append or replace destination table (either 'append' or 'replace')
bl_table_config = 'replace'

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

def load_daily_trans_from_bq(cat, project_id):
    start_time = time.time()
    
    sql_str = """
    with temp as (
            SELECT
                sku_root_id,
                store_id,
                DATE_TRUNC(date, WEEK(MONDAY)) as week_start_date,
                std_price_per_unit, AVG(total_sale_qty) as avg_sales_qty_per_week,
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
    SELECT
        MIN(sku_root_id) as sku_root_id,
        store_id, std_price_per_unit,
        AVG(avg_sales_qty_per_week) as avg_sales_qty,
        AVG(actual_price) as actual_price,
        COUNT(week_start_date) as duration_weeks,
        STDDEV(avg_sales_qty_per_week) as std_dev_sales_qty
    FROM temp
    GROUP BY store_id,
        std_price_per_unit
    ORDER BY sku_root_id
    """.format(c=cat)
    start = time.time()
    
    for i in tqdm(range(1), desc='Loading table...'):
        category_table = pandas_gbq.read_gbq(sql_str, project_id=project_id)
    
    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of distinct sections table from Bigquery {a} mins...".format(a=total_time))
    
    return category_table

if __name__ == "__main__":
    
    start_time = time.time()

    logger.info("Loading input tables from Bigquery....")
    
    # loop through each category
    for each in category:
        
        logger.info("Processing category {a}...".format(a=each))
        
        category_table = load_daily_trans_from_bq(each, project_id)