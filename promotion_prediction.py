import xgboost as xgb
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
import pandas_gbq
from tqdm import tqdm
import time
import logging









def train_promotion_prediction_model(X, y, learning_rate, max_depth, subsample, colsample_bytree, n_estimators, objective,
                                     gamma, alpha, lambda, test_size_perc, test_months_exclusion):

    """train the promotion model during the promotion weeks
    :Args
        X(dataframe): dataframe containing the input data to train the model 
        y(dataframe): dataframe containing the output values to train the model on
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
    
    # Create one hot encoded features on the training data
    cat_columns = ["sku_root_id", "segment", "subcategory", "category", "section", "area", "brand_name", "flag_healthy",
                  "innovation_flag", "tourism_flag", "local_flag", "regional_flag", "Promo_mechanic_en", "customer_profile_type",
                  "marketing_type", ]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=123)
    

