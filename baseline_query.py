import pandas as pd
import pandas_gbq
import logging
from google.cloud import bigquery

# Set logger properties
logger = logging.getLogger('baseline_calculation')

def baseline_dashboard(project_id, dataset_id):
        
        # Load client
        client = bigquery.Client()

        job_config = bigquery.QueryJobConfig()
        
        promo_dashboard_sql = """
        CREATE OR REPLACE TABLE
        `baseline.baseline_promo` 
            PARTITION BY
           date
          CLUSTER BY promo_id, sku_root_id, promo_mechanic
          AS

        WITH 
        bline_end AS (
                    SELECT *, DATE_ADD(DATE_TRUNC(end_date, WEEK(MONDAY)), INTERVAL 7 DAY) AS promo_bline_end_date
                    FROM `gum-eroski-dev.WIP.aggregate_promo_to_sku`), 
        bline_dist AS (
                    SELECT distinct
                    promo_id, promo_year,name, type, start_date, end_date,sku_root_id,store_id,promo_mechanic,discount_depth,promo_bline_end_date
                    FROM bline_end), 

        bline_end_store AS (
                    SELECT bline_end_qty.*,  
                    trans.date,
                    trans.total_sale_qty as s_fw_bl_qty ,
                    trans.total_sale_amt as s_fw_bl_sale ,
                    trans.total_margin_amt as s_fw_bl_margin
                    FROM bline_dist bline_end_qty
                    LEFT JOIN 
                      `ETL.aggregate_weekly_transaction_to_sku` trans
                    ON trans.date >= promo_bline_end_date
                    AND trans.date <= DATE_ADD(promo_bline_end_date, INTERVAL 21 DAY) #(forward period (wks)-1)*7
                    AND trans.sku_root_id = bline_end_qty.sku_root_id
                    AND trans.store_id = bline_end_qty.store_id
                    WHERE trans.date is not null),
        post_promo AS(
                    SELECT 
                    promo_id,CAST(promo_year AS STRING) AS promo_year,sku_root_id,promo_mechanic,discount_depth,date,
                    sum(s_fw_bl_qty) as s_fw_bl_qty,sum(s_fw_bl_sale) as s_fw_bl_sale, sum(s_fw_bl_margin) as s_fw_bl_margin
                    FROM bline_end_store
                    GROUP BY 
                    promo_id,promo_year,sku_root_id,promo_mechanic,discount_depth,date),

        baseline AS(
        SELECT 
        cast(DATE(date) AS DATE) AS date,
        sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth, 
        CAST(change_flag AS NUMERIC) AS change_flag,
        CAST(total_sale_qty AS NUMERIC) as tt_sale_qty_ref,
        CAST(sale_qty_bl AS NUMERIC) AS sale_qty_bl,
        CAST(sale_qty_pct AS NUMERIC) AS sale_qty_pct
        FROM `baseline.baseline` ),

        agg_table as (
        SELECT date, sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth, CONCAT(sku_root_id, "-", promo_id, "-", promo_year, "-", promo_mechanic, "-", discount_depth) AS uniq_id, 
               change_flag, tt_sale_qty_ref,
               CASE WHEN change_flag = 3 THEN post_promo.s_fw_bl_qty ELSE promo.total_sale_qty END AS tt_sale_qty,
               CASE WHEN change_flag = 3 THEN post_promo.s_fw_bl_sale ELSE promo.total_sale_amt END AS tt_sale_amt,
               CASE WHEN change_flag = 3 THEN post_promo.s_fw_bl_margin ELSE promo.total_margin_amt END AS tt_margin_amt,
               sale_qty_bl, sale_qty_bl * std.std_price_per_unit AS sale_amt_bl, sale_qty_bl * std.margin_per_unit AS margin_amt_bl,
               promo.* EXCEPT (date, sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth)
        FROM baseline bl
        LEFT JOIN post_promo
        USING (date, sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth)
        LEFT JOIN (SELECT * EXCEPT (promo_year), CAST(promo_year AS STRING) AS promo_year FROM `ETL.aggregate_promo_to_sku_summary`) promo
        USING (date, sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth)
        LEFT JOIN `ETL.aggregate_std_price_margin` std
        USING (sku_root_id))

        SELECT *, 
        (tt_sale_qty - sale_qty_bl) AS inc_sale_qty, 
        (tt_sale_amt - sale_amt_bl) AS inc_sale_amt, 
        (tt_margin_amt - margin_amt_bl) AS inc_margin_amt
        FROM agg_table


        """
        
        # Create a disctionary to loop over all destination tables and scripts
        baseline_tables = {'baseline_promo': promo_dashboard_sql} 
        
        job_config.write_disposition = "WRITE_TRUNCATE"
        for key in baseline_tables:
                
                # Set the destination table
                table_ref_baseline_dashboard = client.dataset(dataset_id).table(key)
                job_config.destination = table_ref_baseline_dashboard
     
                # Start the query, passing in the extra configuration.
                query_job = client.query(
                    baseline_tables[key],
                    # Location must match that of the dataset(s) referenced in the query
                    # and of the destination table.
                    location='europe-west3',
                    job_config=job_config)  # API request - starts the query

                query_job.result()  # Waits for the query to finish
                logger.info("Completed writing {a} table...".format(a=key))
