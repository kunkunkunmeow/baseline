import pandas as pd
import pandas_gbq
import logging
from google.cloud import bigquery

# Set logger properties
logger = logging.getLogger('forecast_baseline_calculation')

def fcast_baseline_dashboard(project_id, dataset_id):
        
        # Load client
        client = bigquery.Client()

        job_config = bigquery.QueryJobConfig()
        
        fcast_baseline_sql = """
        SELECT 
        DATE(date) as date,
        sku_root_id,
        CAST( b_t_sale_amt AS NUMERIC) as b_t_sale_amt,
        CAST( l_t_sale_amt AS NUMERIC) as l_t_sale_amt,
        CAST( s_t_sale_amt AS NUMERIC) as s_t_sale_amt,
        CAST( y_hat_t_sale_amt AS NUMERIC) as y_hat_t_sale_amt,
        CAST( y_t_sale_amt AS NUMERIC) as y_t_sale_amt,
        CAST( b_t_sale_qty AS NUMERIC) as b_t_sale_qty,
        CAST( l_t_sale_qty AS NUMERIC) as l_t_sale_qty,
        CAST( s_t_sale_qty AS NUMERIC) as s_t_sale_qty,
        CAST( y_hat_t_sale_qty AS NUMERIC) as y_hat_t_sale_qty,
        CAST( y_t_sale_qty AS NUMERIC) as y_t_sale_qty,
        CAST( b_t_margin_amt AS NUMERIC) as b_t_margin_amt,
        CAST( l_t_margin_amt AS NUMERIC) as l_t_margin_amt,
        CAST( s_t_margin_amt AS NUMERIC) as s_t_margin_amt,
        CAST( y_hat_t_margin_amt AS NUMERIC) as y_hat_t_margin_amt,
        CAST( y_t_margin_amt AS NUMERIC) as y_t_margin_amt

        FROM `gum-eroski-dev.baseline_performance.forecast_baseline` 
        """
        
        fcast_metrics_sql = """
        SELECT 
        sku_root_id,
        metric,
        CAST( alpha AS NUMERIC) as alpha,
        CAST( beta AS NUMERIC) as beta,
        CAST( phi AS NUMERIC) as phi,
        CAST( gamma AS NUMERIC) as gamma,
        CAST( l_0 AS NUMERIC) as l_0,
        CAST( b_0 AS NUMERIC) as b_0,
        CAST( SSE AS NUMERIC) as SSE,
        CAST( MSE AS NUMERIC) as MSE,
        CAST( RMSE AS NUMERIC) as RMSE,
        CAST( MAE AS NUMERIC) as MAE,
        CAST( avg_forecast_period AS NUMERIC) as avg_forecast_period,
        CAST( convergence_flag as INT64) as convergence_flag
        FROM `gum-eroski-dev.baseline_performance.forecast_baseline_metrics`
        """
        
        # Create a disctionary to loop over all destination tables and scripts
        tables = {'forecast_baseline':fcast_baseline_sql, 'forecast_baseline_metrics': fcast_metrics_sql} 
        
        job_config.write_disposition = "WRITE_TRUNCATE"
        for key in baseline_tables:
                
                # Set the destination table
                table_ref = client.dataset(dataset_id).table(key)
                job_config.destination = table_ref
     
                # Start the query, passing in the extra configuration.
                query_job = client.query(
                    tables[key],
                    # Location must match that of the dataset(s) referenced in the query
                    # and of the destination table.
                    location='europe-west3',
                    job_config=job_config)  # API request - starts the query

                query_job.result()  # Waits for the query to finish
                logger.info("Completed writing {a} table...".format(a=key))
