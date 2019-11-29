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
from multiprocessing import Process, Manager
import pandas_gbq


# Project ID
project_id = "gum-eroski-dev"
dataset_id = "prediction_results"

# Scope for the prediction (at an area level)
bl_s = "ALIMENTACION"

# Append or replace destination table (either 'append' or 'replace')
bl_table_config = 'replace'

# Set batch size
batchsize = 50

# Set logger properties
logger = logging.getLogger('promotion_prediction_model')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('promotion_prediction.log')
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

# Specify list of input features
# input_features = ['sku_root_id', 'area', 'section', 'category', 'subcategory', 'segment', 'brand_name',
#                   'flag_healthy', 'innovation_flag', 'tourism_flag', 'local_flag', 'regional_flag',
#                   'no_impacted_stores', 'no_impacted_regions', 'avg_store_size', 'Promo_mechanic_en',
#                   'customer_profile_type', 'marketing_type', 'duration', 'includes_weekend', 'campaign_start_day',
#                   'campaign_start_month', 'campaign_start_quarter', 'campaign_start_week', 'leaflet_cover',
#                   'leaflet_priv_space', 'in_leaflet_flag', 'in_gondola_flag', 'in_both_leaflet_gondola_flag',
#                   'discount_depth', 'brand_price_label']

input_features = ['segment', 'brand_name',
                  'no_impacted_stores', 'Promo_mechanic_en',
                  'duration_days',
                  'campaign_start_month', 'campaign_start_week',
                  'discount_depth', 'p_qty_bl']

# Sepcify output features
output_features = ['p_cal_inc_sale_qty']

# Specify exclusion months
test_months_exclusion = ['Jan', 'Aug', 'Nov', 'Dec']

# Specify train, test, forecast
run_config = 'train' # values include 'train', 'train-forecast', 'forecast'

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
def load_t2_from_bq(section, project_id):
    start_time = time.time()

    summary_sql = """
    WITH pr_train AS (
    SELECT distinct promo_mechanic, Promo_mechanic_en , discount_depth 
    from `gum-eroski-dev.prediction_results.prediction_train_input` 
    ), sku_list AS (
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
    fcast.avg_forecast_period as p_qty_bl

    FROM `gum-eroski-dev.ETL.root_sku` sku

    LEFT JOIN `gum-eroski-dev.baseline_performance.forecast_baseline_metrics` fcast

    ON fcast.sku_root_id = sku.sku_root_id

    WHERE fcast.metric = 'pred_baseline_sale_qty'
    and sku.section = "%s"   
    )

    SELECT * from sku_list

    CROSS JOIN pr_train
    """ %(section)
    
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


def run_prediction_model(frame,sku, input_data, train_model, mapping_dict, train_mae, train_mape):

    # Filter on SKUs
    X_apply = input_data[input_data.sku_root_id == sku].reset_index(drop=True)

    # Filter only on the input features
    X_apply = X_apply[input_features] 

    if len(mapping_dict) != 0:

        # Apply mapping on the items in X_apply
        for col in mapping_dict:
            if col in list(X_apply.columns):

                # apply mapping - any new values not in mapping will be set to NaN
                unique_vals_dict = mapping_dict[col]
                X_apply[col] = X_apply[col].map(unique_vals_dict)

    # predict using the model
    pred = train_model.predict(X_apply)

    # compute the prediction intervals (use MAE as a starting point)
    pred_df = pd.DataFrame({output_features[0]: pred[:]})
    pred_df['prediction_interval'] = train_mae
    pred_df['prediction_error_perc'] = train_mape

    # join the results with X_apply
    pred_df = pd.concat([pred_df.reset_index(drop=True), input_data.reset_index(drop=True)], axis = 1)

    # save the results
    frame.append(pred_df)

def train_promotion_prediction_model(input_data, input_features, cat_columns, model, learning_rate, max_depth,
                                     num_leaves, n_iter, n_estimators,
                                     train_size, test_months_exclusion, cat_var_exclusion,
                                     remove_outliers):
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

    # Lets remove data with months Aug, Nov, Dec, Jan from the input data
    if 'campaign_start_month' in list(input_data.columns):
        logger.info("Removing sample data for the following months:\n{}".format(test_months_exclusion))
        input_data = input_data[~input_data['campaign_start_month'].isin(test_months_exclusion)]
        input_data = input_data[~input_data['campaign_start_month'].isin(test_months_exclusion)]

    if remove_outliers:
        logger.info("Removing outliers from sample data...")

        # outlier removal based on negative values
        outliers = input_data[input_data[output_features[0]] <= 0]
        logger.info("Removing all negative values from inc. sales qty, {} sample data points removed...".format(outliers.shape[0]))

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
                X_train[col] = X_train[col].map(unique_vals_dict)
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
                X_train[col] = X_train[col].map(unique_vals_dict)
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

    mae = np.mean(np.absolute(y_validation[output_features[0]].values-pred))
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
    if run_config in ('train', 'train-predict'):
        logger.info("Training the prediction model for the promotion period...")
      
        # obtain input data
        logger.info("Writing historical promotion performance data table in Bigquery...")
        promotion_prediction_query.promotion_prediction_(project_id, dataset_id, bl_s)

        logger.info("Loading historical promotion performance data table from Bigquery...")
        input_data = load_t1_from_bq(project_id)

        # train ML model
        train_model, map_dict, mae, mape = train_promotion_prediction_model(input_data, input_features, cat_columns,
                                                                            model='lightgbm',  # either lightgbm, xgboost, catboost or average
                                                                            learning_rate=0.05, # set between 0.01-0.05
                                                                            max_depth=100, # 100 for lightgbm, 50 for xgboost
                                                                            num_leaves = 250, # for lightgbm
                                                                            n_iter=20000, # for lightgbm, no. of iterations
                                                                            n_estimators = 150, # for xgboost, no of estimators
                                                                            train_size=0.8, # test train split
                                                                            test_months_exclusion=test_months_exclusion, # exclude certain months
                                                                            cat_var_exclusion=False, # exclude specification of categorical variables (lightgbm)
                                                                            remove_outliers=True)  # remove outliers
    
    # Run the model to predict 
    if run_config in ('train-predict'):
        
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
            pred_input_data = load_t2_from_bq(section, project_id)

            logger.info("Computing no. of unique in-scope skus")
            uniq_sku = list(pred_input_data['sku_root_id'].unique())
            logger.info("No. of in-scope skus: {a}".format(a=len(uniq_sku)))

            # Compute the % change values in each of the categories used in the baseline
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
                        p = Process(target=run_prediction_model, args=(frame,sku, pred_input_data, train_model, map_dict, mae, mape))  # Passing the list
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
            logger.info('Completed prediction processing in {a} mins...'.format(a=total_time))

            # upload the final dataframe onto Bigquery
            logger.info('Uploading baseline table to Bigquery...')

            if (i_sec == 0):
                pandas_gbq.to_gbq(results_df, 'prediction_train_input.prediction_promotion_results', project_id=project_id, if_exists=bl_table_config)
            else:
                pandas_gbq.to_gbq(results_df, 'prediction_train_input.prediction_promotion_results', project_id=project_id, if_exists='append')


            logger.info('Completed upload of section prediction to Bigquery...')

        # call function to run query in Bigquery to create baseline related tables
        #logger.info('Creating baseline tables in Bigquery...')
        #baseline_query.baseline_dashboard(project_id, dataset_id)
        #logger.info('Completed creating baseline tables in Bigquery...')

    total_time = round((time.time() - start_time) / 60, 1)
    logger.info('Completed prediction processing in {a} mins...'.format(a=total_time))
