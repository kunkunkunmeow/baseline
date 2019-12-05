import xgboost as xgb
import lightgbm as lgb
from catboost import CatBoostRegressor, Pool, cv
from sklearn.metrics import mean_squared_error, accuracy_score
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
import time
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import promotion_prediction_query
from tqdm import tqdm
import pandas_gbq


# Project ID
project_id = "gum-eroski-dev"
dataset_id = "prediction_results"

# Scope for the prediction (at an area level)
bl_s = "ALIMENTACION"

# Append or replace destination table (either 'append' or 'replace')
bl_table_config = 'replace'

# Set logger properties
logger = logging.getLogger('promotion_prediction_model')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('promotion_prediction.log')
fh.setLevel(logging.DEBUG)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

# Specify list of input features
# input_features = ['sku_root_id', 'area', 'section', 'category', 'subcategory', 'segment', 'brand_name',
#                   'flag_healthy', 'innovation_flag', 'tourism_flag', 'local_flag', 'regional_flag',
#                   'no_impacted_stores', 'no_impacted_regions', 'avg_store_size', 'Promo_mechanic_en',
#                   'customer_profile_type', 'marketing_type', 'duration', 'includes_weekend', 'campaign_start_day',
#                   'campaign_start_month', 'campaign_start_quarter', 'campaign_start_week', 'leaflet_cover',
#                   'leaflet_priv_space', 'in_leaflet_flag', 'in_gondola_flag', 'in_both_leaflet_gondola_flag',
#                   'discount_depth', 'brand_price_label', 'no_hipermercados_stores',	'no_supermercados_stores'	
 #                    'no_gasolineras_stores',	'no_comercio_electronico_stores',	'no_otros_negocio_stores',
 #                     'no_plataformas_stores',	'no_other_stores']

input_features = ['segment', 'brand_name',
                  'no_hipermercados_stores', 'no_supermercados_stores', 'Promo_mechanic_en',
                  'duration_days',
                  'campaign_start_month', 'campaign_start_week',
                  'discount_depth', 'p_qty_bl', 'in_leaflet_flag', 'in_gondola_flag']

# Specify output features
output_features = ['p_cal_inc_avg_qty']

# Specify exclusion months
test_months_exclusion = ['Jan', 'Aug', 'Nov', 'Dec']

# Specify promo mechanics
mechanic = [10,20]

# Specify the min no. of impacted stores
impact_stores_outlier = 20

# Specify the max promo duration
promo_duration_outlier = 21

# Specify the list of discount depths to include
# discount_depths_outlier = ['0% off','2.5% off','5% off','10% off','15% off','20% off','25% off','30% off','35% off','40% off','45% off',
# '50% off','55% off','60% off','65% off','70% off','75% off','80% off','85% off','buy 1 pay 1','buy 2 pay 1','buy 2 pay 1.2',
# 'buy 2 pay 1.3','buy 2 pay 1.4','buy 2 pay 1.5','buy 2 pay 1.53','buy 2 pay 1.6','buy 2 pay 1.7','buy 2 pay 1.8','buy 3 pay 2',
# 'buy 4 pay 3']

discount_depths_outlier = ['2.5% off','5% off','10% off','15% off','20% off','25% off','30% off','35% off','40% off','45% off',
'50% off','55% off','60% off','buy 2 pay 1','buy 2 pay 1.2',
'buy 2 pay 1.3','buy 2 pay 1.4','buy 2 pay 1.5','buy 2 pay 1.6','buy 2 pay 1.7','buy 2 pay 1.8','buy 3 pay 2',
'buy 4 pay 3']

# Specify the in-scope categories to train the model on
segment_outlier = ['QUESO FRESCOS','PIMIENTO PIQUILLO','DENTIFRICOS','YOGURES BASICOS','YOGURES SALUD','CARAMELO','VINOS D.O. TINTOS',
'PIZZAS','VINOS D.O.BLANCOS','SUAVIZANTES ESTANDAR','CERVEZAS EXTRA','ACEITES OLIVA','ACEITE OLIVA VIRGEN','PATATAS FRITAS','PAPEL HIGIENICO SECO',
'SNACKS/APERITIVOS','LECHE LARGA VIDA UHT','AGUAS SIN GAS','GALLETAS SALUD','CERVEZAS 0,0','CAFE MOLIDO','CERVEZAS LAGER','JUDIAS',
'SNACKS','LECHE LARGA VIDA ESPECIALES','VERDURA NATURAL','TOMATE FRITO','C/G NARANJA','CEREALES','CAFE SOLUBLE','PREPARADOS VEGETALES',
'BONITO','ATUN CLARO','MERIENDA','ENERGETICO','GALLETAS MERIENDA','ZUMO Y NECTAR','MAYONESAS, S.FINAS Y S.LIGERAS','GELES FAMILIARES',
'GALLETAS DESAYUNO FAMILIAR','POSTRES ESPECIALES','S/G SABOR','DESAYUNO','ACEITES GIRASOL','YOGURES ESPECIALES','ESPARRAGOS BLANCOS',
'SOLUBLES DE CACAO','RESTO HOGAR','AMBIENTADORES CONTINUOS','C/G COLA','POSTRES BASICOS','TRADICIONALES SIN CASCARA','GALLETAS INFANTILES',
'CREMAS CACAO','ROLLO COCINA','PESCADO NATURAL','CEFALOPODO','SURIMI','TOALLITAS','CHOCOLATE LECHE','LAVAVAJILLAS MAQUINA',
'SOPAS Y CREMAS SOBRE','YOGURES INFANTILES Y PETIT','GARBANZO SECO','LENTEJA SECA','LAVAVAJILLAS MANO','DETERGENTE MAQUINA LIQUIDO',
'LECHE LARGA VIDA BOTELLA','PAN BLANCO','PAN ESPECIALIDADES','PATATAS','DETERGENTE CAPSULAS','CALDOS EN PASTILLAS']

# Specify train, test, forecast
run_config = 'train-predict' # values include 'train', 'train-predict', 'forecast'

# Specify categorical cols
cat_columns = ['sku_root_id', 'description', 'segment', 'subcategory', 'category', 'section', 'area', 'brand_name', 'flag_healthy',
               'innovation_flag', 'tourism_flag', 'local_flag', 'regional_flag', 'Promo_mechanic_en','promo_mechanic','name',
               'start_date', 'end_date', 'customer_profile_type',
               'marketing_type', 'includes_weekend', 'campaign_start_day', 'campaign_start_month',
               'campaign_start_quarter',
               'campaign_start_week', 'leaflet_cover', 'leaflet_priv_space', 'in_leaflet_flag', 'in_gondola_flag',
               'in_both_leaflet_gondola_flag', 'discount_depth', 'period', 'brand_price_label', 'type', 'promo_id', 'promo_year']

# Specify cols to take the avg values
avg_cols = ['sku_root_id', 'segment', 'subcategory', 'category', 'section', 'area', 'brand_name', 'Promo_mechanic_en',
            'brand_price_label',  'discount_depth']


# Function to load distinct section data at a sku level from bigquery
def load_t0_from_bq(area, project_id):
    start_time = time.time()

    summary_sql = """
    SELECT distinct section
    FROM `ETL.root_sku`
    WHERE area = "%s"   """ %(area)

    for i in tqdm(range(1), desc='Loading table...'):
        section_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of distinct sections table from Bigquery {a} mins...".format(a=total_time))

    return section_table
  
# Function to load historical promotion data at a sku + promo mechanic + campaign level from bigquery
def load_t1_from_bq(project_id):
    start_time = time.time()

    summary_sql = """
    SELECT *
    FROM `prediction_results.prediction_train_input`
    """

    for i in tqdm(range(1), desc='Loading table...'):
        hist_promo_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of historical promotion table from Bigquery {a} mins...".format(a=total_time))

    return hist_promo_table

# Function to load prediction data at a sku + promo mechanic from bigquery
def load_t2_from_bq(section, area, project_id):
    start_time = time.time()

    summary_sql = """
    WITH pr_train AS (
    SELECT distinct promo_mechanic, Promo_mechanic_en , discount_depth 
    from `gum-eroski-dev.prediction_results.prediction_train_input` 
    ), 
    input AS (

    SELECT distinct sku_root_id, discount_depth, 1 AS promoted_in_past

    FROM `gum-eroski-dev.prediction_results.prediction_train_input`
    ), 
    fcast_baseline_metrics AS 
    (
    SELECT sku.sku_root_id, 
    AVG(trans.total_sale_qty) as avg_sale_qty_per_store_per_week 
    FROM `gum-eroski-dev.ETL.root_sku` sku
    LEFT JOIN  `gum-eroski-dev.ETL.aggregate_weekly_transaction_to_sku` trans
    ON sku.sku_root_id = trans.sku_root_id
    WHERE promo_flag = FALSE
    GROUP BY sku.sku_root_id
    ),
    sku_list AS (
    SELECT 
    sku.sku_root_id,
    description, 
    area, 
    section, 
    category,
    subcategory,
    segment,
    brand_name,
    CASE WHEN eroskibrand_label is not null 
    THEN eroskibrand_label
    WHEN wealthy_range_flag = 'N' 
    THEN 'Normal'
    WHEN wealthy_range_flag = 'S'
    THEN 'Premium'
    ELSE null
    END AS brand_price_label,
    flag_healthy,
    innovation_flag,
    tourism_flag,
    local_flag,
    regional_flag, 
    50 AS no_hipermercados_stores,	
    50 AS no_supermercados_stores,	
    0 AS no_gasolineras_stores,
    0 AS no_comercio_electronico_stores,
    0 AS no_otros_negocio_stores,
    0 AS no_plataformas_stores,
    0 AS no_other_stores,
    100 AS no_impacted_stores,
    5 AS no_impacted_regions,
    500 AS avg_store_size,
    1 AS type,
    0 AS customer_profile_type,
    'Oferta corporativa' AS marketing_type,
    14 AS duration_days,
    true AS includes_weekend,
    'Thursday' AS campaign_start_day,
    'Jan' AS campaign_start_month,
    1 AS campaign_start_quarter,
    2 AS campaign_start_week,
    0 AS leaflet_cover,
    0 AS leaflet_priv_space,
    1 AS in_leaflet_flag,
    1 AS in_gondola_flag,
    1 AS in_both_leaflet_gondola_flag,
    # 2 weeks worth baseline
    fcast.avg_sale_qty_per_store_per_week*100*2 as p_qty_bl

    FROM `gum-eroski-dev.ETL.root_sku` sku

    LEFT JOIN fcast_baseline_metrics fcast

    ON fcast.sku_root_id = sku.sku_root_id
    
    WHERE sku.area = "%s"
    and sku.section = "%s"   
    ), cj AS (

    SELECT * from sku_list

    CROSS JOIN pr_train
    ) 
    
    SELECT cj.*, input.promoted_in_past  from cj
    LEFT JOIN input 
    
    ON cj.sku_root_id = input.sku_root_id
    AND cj.discount_depth = input.discount_depth
    """ %(area, section)
    
    for i in tqdm(range(1), desc='Loading table...'):
        pred_promo_table = pandas_gbq.read_gbq(summary_sql, project_id=project_id)

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info("Completed loading of prediction promotion table from Bigquery {a} mins...".format(a=total_time))

    return pred_promo_table

def plotImp(model, train_model, X , num = 20):

    if model == 'lightgbm':
        feature_imp = pd.DataFrame({'Value':train_model.feature_importance(),'Feature': X.columns})
    elif model == 'xgboost':
        feature_imp = pd.DataFrame({'Value':train_model.feature_importances_,'Feature': X.columns})

    plt.figure(figsize=(10, 10))
    sns.set(font_scale = 1)
    sns.barplot(x="Value", y="Feature", data=feature_imp.sort_values(by="Value",
                                                        ascending=False)[0:num])
    plt.title('Feature Importance')
    plt.tight_layout()
    plt.show()


def plothist(y_validation, pred):
    plt.subplot(1, 2, 1)
    n, bins, patches = plt.hist(y_validation[output_features[0]].values - pred, 10000, facecolor='red', alpha=0.75)
    plt.xlabel('error (uplift units) xgboost')
    plt.xlim(-10000, 10000)
    plt.title('Histogram of error')

    plt.subplot(1, 2, 2)
    n, bins, patches = plt.hist(y_validation[output_features[0]].values, 10000, facecolor='blue', alpha=0.75)
    plt.xlabel('Error (naive - 0 units uplift)')
    plt.xlim(-10000, 10000)
    plt.title('Histogram of error')
    plt.tight_layout()
    plt.show()

def run_prediction_model_single(input_data, train_model, mapping_dict, train_mae, train_mape):
    
    # convert input data format
    # for cols in input data that is not in the cat cols list, convert to numeric
    for col in list(input_data.columns):
            if col not in list(cat_columns):
                input_data[col] = pd.to_numeric(input_data[col])
    
    X_apply = input_data
    logger.info("Sample data includes {b} samples to predict...".format(b=X_apply.shape[0]))

    # Filter only on the input features
    X_apply = X_apply[input_features]
    
    logger.info("Applying mapping to sample data...")
    if len(mapping_dict) != 0:

        # Apply mapping on the items in X_apply
        for col in mapping_dict:
            if col in list(X_apply.columns):

                # apply mapping - any new values not in mapping will be set to NaN
                unique_vals_dict = mapping_dict[col]#
                X_apply = X_apply.copy()
                X_apply[col] = X_apply[col].map(unique_vals_dict)

    # predict using the model
    logger.info("Predicting target variable for sample data...")
    pred = train_model.predict(X_apply)

    # compute the prediction intervals (use MAE as a starting point)
    pred_df = pd.DataFrame({output_features[0]: pred[:]})
    pred_df['prediction_interval'] = train_mae
    pred_df['prediction_error_perc'] = train_mape

    # join the results with X_apply
    pred_df = pd.concat([pred_df.reset_index(drop=True), input_data.reset_index(drop=True)], axis = 1)

    # save the results
    logger.info("Completed prediction of target variable...")
    return pred_df

def train_promotion_prediction_model(input_data, input_features, cat_columns, model, learning_rate, max_depth,
                                     num_leaves, n_iter, n_estimators,
                                     train_size, test_months_exclusion, cat_var_exclusion,
                                     remove_outliers, impact_stores_outlier=None, promo_duration_outlier=None,
                                     discount_depths_outlier=None, segment_outlier=None):
    """train the promotion model during the promotion weeks
    :Args
        X(dataframe): dataframe containing the input data to train the model
        y(dataframe): dataframe containing the output values to train the model on
        input_features(list): list of cols that we will be using
        cat_columns(list): list of cols that are categorical
        learning_rate(float): step size shrinkage used to prevent overfitting. Range is [0,1]
        max_depth(int): determines how deeply each tree is allowed to grow during any boosting round
        subsample(float): percentage of samples used per tree. Low value can lead to underfitting
        colsample_bytree(float): percentage of features used per tree. High value can lead to overfitting
        n_iter(int): number of iterations you want to run
        train_size_perc(float): train test splits
        test_months_exclusion: identifies the months to be excluded from the training data set
    :return:
        xgboost model(model): xgboost ML model
    """
    
    # convert input data format
    # for cols in input data that is not in the cat cols list, convert to numeric
    for col in list(input_data.columns):
            if col not in list(cat_columns):
                input_data[col] = pd.to_numeric(input_data[col])
    
    # Check no. of input sample data rows
    logger.info("Input sample data includes {b} samples...".format(b=input_data.shape[0]))
    
    # Lets remove data within the test exclusion months list
    if 'campaign_start_month' in list(input_data.columns) and test_months_exclusion is not None:
        outliers = input_data[input_data.campaign_start_month.isin(test_months_exclusion)]
        logger.info("Removing sample data where campaign start months is in {a}, {b} sample data points removed...".format(a=test_months_exclusion,
                                                                                                                 b=outliers.shape[0]))
        input_data = input_data[~input_data['campaign_start_month'].isin(test_months_exclusion)]
    
     # Lets remove data where store count is below a certain value
    if 'no_impacted_stores' in list(input_data.columns) and impact_stores_outlier is not None:
        outliers = input_data[input_data['no_impacted_stores'] < impact_stores_outlier]
        logger.info("Removing sample data where impacted stores < {a}, {b} sample data points removed...".format(a=impact_stores_outlier,
                                                                                                                 b=outliers.shape[0]))
        input_data = input_data[input_data['no_impacted_stores'] >= impact_stores_outlier]
    
    # Lets remove data where duration is above a certain value
    if 'duration_days' in list(input_data.columns) and promo_duration_outlier is not None:
        outliers = input_data[input_data['duration_days'] > promo_duration_outlier]
        logger.info("Removing sample data where promotion duration > {a}, {b} sample data points removed...".format(a=promo_duration_outlier,
                                                                                                                 b=outliers.shape[0]))
        input_data = input_data[input_data['duration_days'] <= promo_duration_outlier]
  
    # Lets remove data where discount depth is not in specified list
    if 'discount_depth' in list(input_data.columns) and discount_depths_outlier is not None:
        outliers = input_data[~input_data.discount_depth.isin(discount_depths_outlier)]
        logger.info("Removing sample data where discount depth is not in {a}, {b} sample data points removed...".format(a=discount_depths_outlier,
                                                                                                                 b=outliers.shape[0]))
        input_data = input_data[input_data.discount_depth.isin(discount_depths_outlier)]
    
    # Lets remove data where segment is not in in-scope segment
    if 'segment' in list(input_data.columns) and segment_outlier is not None:
        outliers = input_data[~input_data.segment.isin(segment_outlier)]
        logger.info("Removing sample data where segment is not in {a}, {b} sample data points removed...".format(a=segment_outlier,
                                                                                                                 b=outliers.shape[0]))
        input_data = input_data[input_data.segment.isin(segment_outlier)]

    if remove_outliers:
        logger.info("Removing outliers from sample data...")
        
        # outlier removal based on negative values
        outliers = input_data[input_data[output_features[0]] <= 0]
        logger.info("Removing all negative values from {a}, {b} sample data points removed...".format(a=output_features[0],
                                                                                                      b=outliers.shape[0]))

        input_data = input_data[input_data[output_features[0]] > 0]

        # outlier removal based on quantile in target variable
        q = input_data[output_features[0]].quantile(0.95)

        outliers = input_data[input_data[output_features[0]] >= q]
        logger.info("Based on 95% quantiles, {} sample data points removed...".format(outliers.shape[0]))

        input_data = input_data[input_data[output_features[0]] < q]
        

    # Filter on only the input features
    X = input_data[input_features]
    y = input_data[output_features]

    # Check absent values
    null_value_stats_x = X.isnull().sum(axis=0)
    logger.info("Null values for input features include:\n{}".format(null_value_stats_x[null_value_stats_x != 0]))

    null_value_stats_y = y.isnull().sum(axis=0)
    logger.info("Null values for target variable include:\n{}".format(null_value_stats_y[null_value_stats_y != 0]))

    # Throw error if any values are null in y
    if y.isnull().values.any():
        logger.error("Null values found in target data...")
        raise ValueError('Null values found in target data-frame: y')

    # Fill remaining absent values in X with -999
    X.fillna(-999, inplace=True)

    # Check data types
    logger.info("Input dataset data types include:\n{}".format(X.dtypes))
    logger.info("Target variable data types include:\n{}".format(y.dtypes))

    logger.info("Target variable mean is: {}".format(y[output_features[0]].mean()))
    logger.info("Target variable stdev is: {}".format(y[output_features[0]].std()))

    # Lets split the data into training and validation sets
    X_train, X_validation, y_train, y_validation = train_test_split(X, y, train_size=train_size, random_state=42)
    logger.info("Training dataset includes {} samples...".format(X_train.shape[0]))
    logger.info("Test dataset includes {} samples...".format(X_validation.shape[0]))

    # create a mapping dictionary (to be used for models which require int categorical cols)
    map_dict = {}

    if model == 'CatBoost':

        # using the catboost model
        params = {'depth': [4, 7, 10],
                  'learning_rate': [0.03, 0.1, 0.15],
                  'l2_leaf_reg': [1, 4, 9],
                  'iterations': [300]}

        # Obtain categorical feature index
        cat_features_index = [X.columns.get_loc(c) for c in cat_columns if c in X]

        # initialise CatBoost regressor
        train_model = CatBoostRegressor(iterations=700,
                                        learning_rate=learning_rate,
                                        depth=max_depth,
                                        eval_metric='RMSE',
                                        random_seed=42,
                                        bagging_temperature=0.2,
                                        od_type='Iter',
                                        metric_period=75,
                                        od_wait=100)

        # Fit the model - catboost does not require us to specify integers for cat features
        train_model.fit(X_train, y_train,
                        eval_set=(X_validation, y_validation),
                        cat_features=cat_features_index,
                        use_best_model=True)

        # Calculate feature importance
        fea_imp = pd.DataFrame({'imp': train_model.feature_importances_, 'col': X.columns})
        fea_imp = fea_imp.sort_values(['imp', 'col'], ascending=[True, False])
        #         fea_imp.plot(kind='barh', x='col', y='imp', figsize=(10, 7), legend=None)
        #         plt.title('CatBoost - Feature Importance')
        #         plt.ylabel('Features')
        #         plt.xlabel('Importance');

        # Save model
        train_model.save_model('catboost_model.dump')

    elif model =='lightgbm':

        # For lightgbm, we need to convert our categorical features to int
        # Loop through categorical cols
        for col in cat_columns:
            if col in list(X.columns):

                # get unique values
                unique_vals = X[col].unique()
                unique_vals_dict = dict([(val, num + 1) for num, val in enumerate(unique_vals)])

                # map them for the train and test data sets
                X_train = X_train.copy()
                X_train[col] = X_train[col].map(unique_vals_dict)
                X_validation = X_validation.copy()
                X_validation[col] = X_validation[col].map(unique_vals_dict)

                # store the mapping for later use
                map_dict[col] = unique_vals_dict

        # LightGBM dataset formatting (with categorical variables)
        if cat_var_exclusion:
            lgtrain = lgb.Dataset(X_train, y_train,
                                  feature_name=input_features)
            lgvalid = lgb.Dataset(X_validation, y_validation,
                                  feature_name=input_features)
        else:
            cat_col = [col for col in cat_columns if col in list(X.columns)]

            lgtrain = lgb.Dataset(X_train, y_train,
                                  feature_name=input_features,
                                  categorical_feature=cat_col)
            lgvalid = lgb.Dataset(X_validation, y_validation,
                                  feature_name=input_features,
                                  categorical_feature=cat_col)

        params = {
            'objective': 'regression',
            'metric': 'rmse',
            'num_leaves': num_leaves,
            'max_depth': max_depth,
            'learning_rate': learning_rate,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 1,
            'boosting_type': 'gbdt',
            'verbosity': -1
        }

        train_model = lgb.train(
            params,
            lgtrain,
            num_boost_round=n_iter,
            valid_sets=[lgtrain, lgvalid],
            valid_names=["train", "valid"],
            early_stopping_rounds=1000,
            verbose_eval=500
        )

        pred = train_model.predict(X_validation)

    elif model == 'xgboost':

        # For xgboost, we need to convert our categorical features to int
        # There are 3 approaches - one-hot encode, label encode and binary encode

        # Here, for simplicity, we are using label encoders
        # Loop through categorical cols
        for col in cat_columns:
            if col in list(X.columns):
                # get unique values
                unique_vals = X[col].unique()
                unique_vals_dict = dict([(val, num + 1) for num, val in enumerate(unique_vals)])

                # map them for the train and test data sets
                X_train = X_train.copy()
                X_train[col] = X_train[col].map(unique_vals_dict)
                X_validation = X_validation.copy()
                X_validation[col] = X_validation[col].map(unique_vals_dict)

                # store the mapping for later use
                map_dict[col] = unique_vals_dict

        train_model = xgb.XGBRegressor(objective='reg:linear',
                                 colsample_bytree=0.3,
                                 learning_rate=learning_rate,
                                 max_depth=max_depth,
                                 alpha=10,
                                 n_estimators=n_estimators,
                                 verbosity=2)

        train_model.fit(X_train, y_train)

        pred = train_model.predict(X_validation)

    elif model=='average':

        # compute the model average - use avg_cols
        avg_c = [col for col in avg_cols if col in list(X.columns)]

        # add the target variable col
        tot_c = avg_c + output_features

        # combine two dataframes into 1
        X_train_avg = pd.concat([X_train, y_train], ignore_index=True, sort=False, axis=1)
        X_train_avg = X_train_avg[tot_c]

        # group by the avg_c
        X_train_avg = X_train_avg.groupby(avg_c).avg()

        # test accuracy on test set
        # ensure all values in test set are in train set
        y_val_avg = pd.concat([X_train, y_train], ignore_index=True, sort=False, axis=1)
        y_val_avg = pd.merge(y_val_avg, X_train_avg, on=avg_c, how='left')
        #pred = y_val_avg[]

    # Evaluate the model
    rmse = np.sqrt(mean_squared_error(y_validation, pred))
    logger.info("RMSE: {}".format(rmse))

    mean_error = np.mean(y_validation[output_features[0]].values-pred)
    logger.info("Mean Error: {}".format(mean_error))

    mae = np.median(np.absolute(y_validation[output_features[0]].values-pred))
    logger.info("MAE: {}".format(mae))

    mape = np.divide(y_validation[output_features[0]].values - pred, y_validation[output_features[0]].values)
    mape[mape == np.inf] = 0
    mape[mape == -np.inf] = 0
    mape = np.median(np.abs(mape))
    logger.info("MAPE: {}%".format(mape*100))

    val_std = np.std(y_validation[output_features[0]].values)
    logger.info("Benchmark STD: {}".format(val_std))

    val_mean = np.mean(y_validation[output_features[0]].values)
    logger.info("Benchmark Mean Error: {}".format(val_mean))

    val_mae = np.mean(np.absolute(y_validation[output_features[0]].values))
    logger.info("Benchmark MAE: {}".format(val_mae))

    logger.info("Benchmark MAPE: -100%")

    # plot the results
    #plothist(y_validation, pred)

    # plot the feature importance
    #plotImp(model, train_model, X, num=20)

    return train_model, map_dict, mae, mape
            
if __name__ == "__main__":
    
    start_time = time.time()
    
    # Train the model
    if run_config == 'train' or run_config == 'train-predict':
        logger.info("Training the prediction model for the promotion period...")
      
        # obtain input data
        logger.info("Writing historical promotion performance data table in Bigquery...")
        promotion_prediction_query.promotion_prediction_(project_id, dataset_id, bl_s, mechanic)

        logger.info("Loading historical promotion performance data table from Bigquery...")
        input_data = load_t1_from_bq(project_id)

        # train ML model
        train_model, map_dict, mae, mape = train_promotion_prediction_model(input_data, input_features, cat_columns,
                                                                            model='lightgbm',  # either lightgbm, xgboost, catboost or average
                                                                            learning_rate=0.03, # set between 0.01-0.05
                                                                            max_depth=200, # 100 for lightgbm, 50 for xgboost
                                                                            num_leaves = 250, # for lightgbm
                                                                            n_iter=10000, # for lightgbm, no. of iterations, 20000
                                                                            n_estimators = 150, # for xgboost, no of estimators
                                                                            train_size=0.8, # test train split
                                                                            test_months_exclusion=None, # exclude certain months
                                                                            cat_var_exclusion=False, # exclude specification of categorical variables (lightgbm)
                                                                            remove_outliers=True,
                                                                            impact_stores_outlier=impact_stores_outlier, 
                                                                            promo_duration_outlier=promo_duration_outlier,
                                                                            discount_depths_outlier=discount_depths_outlier,
                                                                            segment_outlier=None)  # remove outliers
    
    # Run the model to predict 
    if run_config == 'train-predict':
        
        logger.info("Predicting the promotional performance for in-scope skus....")
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

            # Load the input data table for each section   
            logger.info("Loading prediction input table from Bigquery....")
            pred_input_data = load_t2_from_bq(section, bl_s, project_id)

            logger.info("Computing no. of unique in-scope skus")
            uniq_sku = list(pred_input_data['sku_root_id'].unique())
            logger.info("No. of in-scope skus: {a}".format(a=len(uniq_sku)))
                
            # use a single proc
            results_df = run_prediction_model_single(pred_input_data, train_model, map_dict, mae, mape)
            logger.info('Results dataframe has {a} rows and {b} cols...'.format(a=results_df.shape[0], b=results_df.shape[1]))

            # Convert all nulls to None
            results_df = results_df.where((pd.notnull(results_df)), None)

            total_time = round((time.time() - section_start_time) / 60, 1)
            logger.info('Completed prediction processing in {a} mins...'.format(a=total_time))

            # upload the final dataframe onto Bigquery
            logger.info('Uploading prediction results table to Bigquery...')

            if (i_sec == 0):
                pandas_gbq.to_gbq(results_df, 'prediction_results.prediction_promotion_results', project_id=project_id, if_exists=bl_table_config)
            else:
                pandas_gbq.to_gbq(results_df, 'prediction_results.prediction_promotion_results', project_id=project_id, if_exists='append')


            logger.info('Completed upload of section prediction to Bigquery...')

        # call function to run query in Bigquery to create baseline related tables
        logger.info('Creating prediction promotion top 20 table in Bigquery...')
        promotion_prediction_query.promotion_prediction_res(project_id, dataset_id)
        logger.info('Completed creating prediction promotion top 20 table in Bigquery...')

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info('Completed prediction processing in {a} mins...'.format(a=total_time))
