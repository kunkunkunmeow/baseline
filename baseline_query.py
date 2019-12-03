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
        WITH 
        bline_end AS (
                    SELECT * EXCEPT (discount_depth, no_to_pay, no_to_buy), 
                    IFNULL(discount_depth, 'ISNULL') AS discount_depth, 
                    IFNULL(CAST(no_to_pay AS STRING), 'ISNULL') AS no_to_pay, 
                    IFNULL(CAST(no_to_buy  AS STRING), 'ISNULL') AS no_to_buy, 
                    DATE_ADD(DATE_TRUNC(end_date, WEEK(MONDAY)), INTERVAL 7 DAY) AS promo_bline_end_date
                    FROM `gum-eroski-dev.WIP.aggregate_promo_to_sku`), 
        bline_dist AS (
                    SELECT distinct
                    promo_id, promo_year, sku_root_id, store_id, promo_mechanic, discount_depth, no_to_pay, no_to_buy, 
                    promo_bline_end_date
                    FROM bline_end), 

        bline_end_store AS (
                    SELECT bline_end_qty.*,  
                    trans.date,
                    trans.total_sale_qty as s_fw_bl_qty ,
                    trans.total_sale_amt as s_fw_bl_sale ,
                    trans.total_margin_amt as s_fw_bl_margin,
                    (trans.total_price_if_sku_std_price -  trans.total_sale_amt) as s_fw_bl_discount
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
                    promo_id,CAST(promo_year AS STRING) AS promo_year,sku_root_id,promo_mechanic,discount_depth, no_to_pay, no_to_buy, date,
                    sum(s_fw_bl_qty) as s_fw_bl_qty,sum(s_fw_bl_sale) as s_fw_bl_sale, sum(s_fw_bl_margin) as s_fw_bl_margin, sum(s_fw_bl_discount) as s_fw_bl_discount
                    FROM bline_end_store
                    GROUP BY 
                    promo_id,promo_year,sku_root_id,promo_mechanic,discount_depth,no_to_pay, no_to_buy, date),

        baseline AS(
        SELECT 
        cast(DATE(date) AS DATE) AS date,
        uniq_id, sku_root_id, promo_id, promo_year, promo_mechanic, 
        IFNULL(discount_depth, 'ISNULL') AS discount_depth, 
        IFNULL(no_to_pay, 'ISNULL') AS no_to_pay, 
        IFNULL(no_to_buy, 'ISNULL') AS no_to_buy,
        CAST(change_flag AS NUMERIC) AS change_flag,
        CAST(total_sale_qty AS NUMERIC) as tt_sale_qty_ref,
        CAST(sale_qty_bl AS NUMERIC) AS sale_qty_bl,
        CAST(sale_qty_pct AS NUMERIC) AS sale_qty_pct
        FROM `baseline.baseline` ),

        agg_table as (
        SELECT date,  uniq_id, sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth, no_to_pay, no_to_buy,
               change_flag, tt_sale_qty_ref, sale_qty_pct,
               CASE WHEN change_flag = 3 THEN post_promo.s_fw_bl_qty ELSE promo.total_sale_qty END AS tt_sale_qty,
               CASE WHEN change_flag = 3 THEN post_promo.s_fw_bl_sale ELSE promo.total_sale_amt END AS tt_sale_amt,
               CASE WHEN change_flag = 3 THEN post_promo.s_fw_bl_margin ELSE promo.total_margin_amt END AS tt_margin_amt,
               CASE WHEN change_flag = 3 THEN post_promo.s_fw_bl_discount ELSE promo.total_discount_amt END AS tt_discount,
               sale_qty_bl, sale_qty_bl * std.std_price_per_unit AS sale_amt_bl, sale_qty_bl * std.margin_per_unit AS margin_amt_bl,
               promo.* EXCEPT (date, sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth, no_to_pay, no_to_buy)
        FROM baseline bl
        LEFT JOIN post_promo
        USING (date, sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth, no_to_pay, no_to_buy)
        LEFT JOIN (SELECT * EXCEPT (promo_year, discount_depth, no_to_pay, no_to_buy), 
                    CAST(promo_year AS STRING) AS promo_year, 
                    IFNULL(discount_depth, 'ISNULL') AS discount_depth, 
                    IFNULL(CAST(no_to_pay AS STRING), 'ISNULL') AS no_to_pay, 
                    IFNULL(CAST(no_to_buy  AS STRING), 'ISNULL') AS no_to_buy
                   FROM `ETL.aggregate_promo_to_sku_summary`) promo
        USING (date, sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth, no_to_pay, no_to_buy)
        LEFT JOIN `ETL.aggregate_std_price_margin` std
        USING (sku_root_id)),
        
        uniq_id_table as (
        SELECT DISTINCT * EXCEPT (date,  change_flag, includes_weekend, tt_sale_qty_ref, sale_qty_pct, tt_sale_qty, tt_sale_amt, tt_margin_amt, 
                                  sale_qty_bl, sale_amt_bl, margin_amt_bl, total_sale_amt, total_margin_amt, total_sale_qty, s_prev_bl_qty, total_discount_amt)
        FROM agg_table 
        WHERE change_flag <>3),
        
        final_tb as (
        SELECT date, sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth, no_to_pay, no_to_buy, 
               change_flag, includes_weekend, tt_sale_qty_ref, sale_qty_pct, tt_sale_qty, tt_sale_amt, tt_margin_amt, 
               sale_qty_bl, sale_amt_bl, margin_amt_bl, total_sale_amt, total_margin_amt, total_sale_qty, s_prev_bl_qty, total_discount_amt,
               uniq_id_table.* EXCEPT(sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth, no_to_pay, no_to_buy)
        FROM agg_table
        LEFT JOIN uniq_id_table
        USING (sku_root_id, promo_id, promo_year, promo_mechanic, discount_depth, no_to_pay, no_to_buy))
        
        SELECT * EXCEPT(includes_weekend),
        IFNULL(includes_weekend, true) as includes_weekend,
        (tt_sale_amt - sale_amt_bl) as inc_sale_amt,
        (tt_sale_qty - sale_qty_bl) as inc_sale_qty,
        (tt_margin_amt - margin_amt_bl) as inc_margin_amt
        FROM final_tb
   
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
