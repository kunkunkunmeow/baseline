import pandas as pd
import pandas_gbq

# Project ID
project_id = "gum-eroski-dev"

word = "date"
date = "'2019-04-15'"

sql = f""" CREATE OR REPLACE TABLE `gum-eroski-dev.WIP.test2`  AS
       SELECT * FROM `gum-eroski-dev.WIP.baseline_dashboard` 
       WHERE {word} = {date}; 
       
       CREATE OR REPLACE TABLE `gum-eroski-dev.WIP.test3` AS
       SELECT * FROM `gum-eroski-dev.WIP.baseline_dashboard` 
       WHERE {word} = {date};"""
       
test = pandas_gbq.read_gbq(sql, project_id=project_id) 
