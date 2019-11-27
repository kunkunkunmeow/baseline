import xgboost as xgb
import lightgbm as lgb
from catboost import CatBoostRegressor, Pool, cv
from sklearn.metrics import mean_squared_error, accuracy_score
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
import pandas_gbq
from tqdm import tqdm
import time
import logging

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
input_features = ['sku_root_id', 'area', 'section', 'category', 'subcategory', 'segment', 'brand_name', 
                  'flag_healthy', 'innovation_flag', 'tourism_flag', 'local_flag', 'regional_flag', 
                  'no_impacted_stores', 'no_impacted_regions', 'avg_store_size', 'Promo_mechanic_en',
                  'customer_profile_type', 'marketing_type', 'duration', 'includes_weekend', 'campaign_start_day',
                  'campaign_start_month', 'campaign_start_quarter', 'campaign_start_week', 'leaflet_cover', 
                  'leaflet_priv_space', 'in_leaflet_flag', 'in_gondola_flag', 'in_both_leaflet_gondola_flag', 
                  'discount_depth', 'brand_price_label']

output_features = ['pct_inc_qty']
    
# Specify categorical cols
cat_columns = ['sku_root_id', 'segment', 'subcategory', 'category', 'section', 'area', 'brand_name', 'flag_healthy',
               'innovation_flag', 'tourism_flag', 'local_flag', 'regional_flag', 'Promo_mechanic_en', 'customer_profile_type',
               'marketing_type', 'includes_weekend', 'campaign_start_day', 'campaign_start_month', 'campaign_start_quarter', 
               'campaign_start_week', 'leaflet_cover', 'leaflet_priv_space', 'in_leaflet_flag', 'in_gondola_flag', 
               'in_both_leaflet_gondola_flag', 'discount_depth', 'brand_price_label']


def train_promotion_prediction_model(input_data, input_features, cat_columns, model, learning_rate, max_depth, subsample, colsample_bytree, n_estimators, objective,
                                     gamma, alpha, lambda, test_size_perc, test_months_exclusion):

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
        n_estimators(int): number of trees you want to build
        objective(reg object): determines the loss function to be used like reg:linear for regression problems,
        gamma(int): controls whether a given node will split based on the expected reduction in loss after the split. A higher value leads to fewer splits. Supported only for tree-based learners
        alpha(int): L1 regularization on leaf weights. A large value leads to more regularization
        lambda(int): L2 regularization on leaf weights and is smoother than L1 regularization
        test_size_perc(float): train test splits
        test_months_exclusion: identifies the months to be excluded from the training data set
    :return:
        xgboost model(model): xgboost ML model
    """
    
    # Filter on only the input features
    X = input_data[input_features]
    y = input_data[output_features]
    
    # Check absent values
    null_value_stats_x = X.isnull().sum(axis=0)
    logger.info("Null values for input features include:")
    logger.info("\n{}".format(null_value_stats_x[null_value_stats_x != 0]))
    
    null_value_stats_y = y.isnull().sum(axis=0)
    logger.info("Null values for predicted features include:")
    logger.info("\n{}".format(null_value_stats_y[null_value_stats_y != 0]))
    
    # Remove rows where we have null in y 
    # TODO
    
    # Fill remaining absent values in X with -999 
    X.fillna(-999, inplace=True)
    
    # Check data types
    logger.info("Input dataset data types include:")
    logger.info("\n{}".format(X.dtypes))
    
    # Visualise the probability distribution of the target variable (we would like something uniform)
    # TODO
    
    # Obtain categorical feature index
    cat_features_index = [X.columns.get_loc(c) for c in cat_columns if c in X]
    
    # Lets split the data into training and validation sets
    X_train, X_validation, y_train, y_validation = train_test_split(X, y, train_size=0.8, random_state=42)
    
    # Lets further remove data with months Aug, Nov, Dec, Jan from train and test set
    # TODO
    
    if model='CatBoost':
      
        # using the catboost model
        params = {'depth': [4, 7, 10],
          'learning_rate' : [0.03, 0.1, 0.15],
         'l2_leaf_reg': [1,4,9],
         'iterations': [300]}
        
        # initialise CatBoost regressor
        model =  CatBoostRegressor(iterations=700,
                             learning_rate=0.01,
                             depth=16,
                             eval_metric='RMSE',
                             random_seed = 42,
                             bagging_temperature = 0.2,
                             od_type='Iter',
                             metric_period = 75,
                             od_wait=100)
        
        
        # Fit the model - catboost does not require us to specify integers for cat features
        model.fit(X_train, y_train,
                 eval_set=(X_validation, y_validation),
                 cat_features=cat_features_index,
                 use_best_model=True)
        
        # Calculate feature importance
        fea_imp = pd.DataFrame({'imp': model.feature_importances_, 'col': X.columns})
        fea_imp = fea_imp.sort_values(['imp', 'col'], ascending=[True, False])
#         fea_imp.plot(kind='barh', x='col', y='imp', figsize=(10, 7), legend=None)
#         plt.title('CatBoost - Feature Importance')
#         plt.ylabel('Features')
#         plt.xlabel('Importance');


        # Save model
        model.save_model('catboost_model.dump')
            

