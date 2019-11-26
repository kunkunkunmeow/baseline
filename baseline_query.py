import pandas as pd
import pandas_gbq
from google.cloud import bigquery



def baseline_dashboard(project_id, dataset_id):
        
        # Load client
        client = bigquery.Client()

        job_config = bigquery.QueryJobConfig()
        
        baseline_dashboard_sql = """

        # Create baseline dashboard
        WITH agg_weekly as 
        (SELECT date, sku_root_id, description, area, section, category, subcategory, segment, 
                CASE WHEN promo_flag_binary = 1 THEN (total_price_if_sku_std_price  - total_sale_amt) ELSE NULL END AS total_discount
        from `ETL.aggregate_weekly_transaction_summary`),

        baseline_temp as (
        SELECT 
        cast(DATE(date) AS DATE) AS date,
        sku_root_id ,
        promo_flag_binary ,
        change_flag ,
        CAST(total_sale_amt AS NUMERIC) AS total_sale_amt,
        CAST(sale_amt_bl AS NUMERIC) AS sale_amt_bl ,
        CAST(sale_amt_bl_ext  AS NUMERIC) AS sale_amt_bl_ext ,
        CAST(total_sale_qty  AS NUMERIC) AS total_sale_qty ,
        CAST(sale_qty_bl  AS NUMERIC) AS sale_qty_bl ,
        CAST(sale_qty_bl_ext  AS NUMERIC) AS sale_qty_bl_ext ,
        CAST(total_margin_amt  AS NUMERIC) AS total_margin_amt ,
        CAST(margin_amt_bl  AS NUMERIC) AS margin_amt_bl ,
        CAST(margin_amt_bl_ext  AS NUMERIC) AS margin_amt_bl_ext ,
        CAST(sale_amt_promo_flag  AS NUMERIC) AS sale_amt_promo_flag ,
        CAST(sale_qty_promo_flag  AS NUMERIC) AS sale_qty_promo_flag ,
        CAST(margin_amt_promo_flag  AS NUMERIC) AS margin_amt_promo_flag,
        CAST(incremental_sale   AS NUMERIC) AS incremental_sale_amt ,
        CAST(incremental_margin   AS NUMERIC) AS incremental_margin_amt,
        CAST(incremental_qty   AS NUMERIC) AS incremental_sale_qty
        FROM `baseline_performance.baseline`  ),

        baseline as (
        SELECT *,
        CASE WHEN change_flag =3 THEN total_sale_amt - sale_amt_bl_ext ELSE incremental_sale_amt END AS ttl_inc_sale_amt,
        CASE WHEN change_flag =3 THEN total_sale_qty - sale_qty_bl_ext ELSE incremental_sale_qty END AS ttl_inc_sale_qty,
        CASE WHEN change_flag =3 THEN total_margin_amt - margin_amt_bl_ext ELSE incremental_margin_amt END AS ttl_inc_margin_amt,
        CASE WHEN change_flag=3 THEN sale_amt_bl_ext ELSE sale_amt_bl END as ttl_sale_amt_bl,
        CASE WHEN change_flag=3 THEN sale_qty_bl_ext ELSE sale_qty_bl END as ttl_sale_qty_bl,
        CASE WHEN change_flag=3 THEN margin_amt_bl_ext ELSE margin_amt_bl END as ttl_margin_amt_bl
        FROM baseline_temp),

        brand AS(
        SELECT sku_root_id, brand_name , eroskibrand_flag, eroskibrand_label 
        FROM `ETL.root_sku`)

        SELECT * EXCEPT (total_discount),
        CASE WHEN total_discount <0 THEN 0 ELSE total_discount END AS discount,
        ttl_inc_sale_amt/NULLIF(ABS(ttl_sale_amt_bl),0) as pct_inc_sale,
        ttl_inc_sale_qty/NULLIF(ABS(ttl_sale_qty_bl),0) as pct_inc_qty,
        ttl_inc_margin_amt/NULLIF(ABS(ttl_margin_amt_bl),0) as pct_inc_margin
        FROM agg_weekly
        INNER JOIN baseline
        USING (date, sku_root_id)
        LEFT JOIN brand
        USING (sku_root_id)
        ;
        """
        
        promo_dashboard_sql = """


        # create baseline_promo table
        WITH 
        promo as (
                  select * 
                  from `ETL.aggregate_promo_to_sku` 
                  INNER JOIN 
                  (SELECT sku_root_id, date, COUNT(*) as promo_weight
                  FROM `ETL.aggregate_promo_to_sku` 
                  GROUP BY sku_root_id , date)
                  USING (sku_root_id, date)),

        ---change the extended baseline date to the last promotion date
        baseline_date as (
                 select sku_root_id, date as date, LAST_VALUE(date_temp IGNORE NULLS) OVER (ORDER BY sku_root_id, date) as date_new
                 FROM 
                 (SELECT sku_root_id, date,
                 CASE WHEN change_flag =3 THEN NULL ELSE date END as date_temp
                 FROM `baseline_performance.baseline_dashboard` )),

        baseline as(
                  SELECT 
                  bl_date.sku_root_id, bl_date.date_new as date, promo_flag_binary, change_flag, discount,
                  total_sale_amt,   ttl_sale_amt_bl,       ttl_inc_sale_amt,     pct_inc_sale,
                  total_sale_qty,   ttl_sale_qty_bl,       ttl_inc_sale_qty,     pct_inc_qty,
                  total_margin_amt, ttl_margin_amt_bl,     ttl_inc_margin_amt,   pct_inc_margin
                  from `baseline_performance.baseline_dashboard` baseline
                  INNER JOIN baseline_date bl_date
                  USING(date, sku_root_id)
                  where change_flag in (1,2,3)
                  )

        SELECT date, 
        sku_root_id, 
        promo.description, 
        promo.area, 
        promo.section , 
        promo.category , 
        promo.subcategory , 
        promo.segment , 
        promo.brand_name, 
        promo.eroskibrand_flag,
        promo.eroskibrand_label,
        promo.wealthy_range_flag,
        promo.flag_healthy,
        promo.innovation_flag,
        promo.tourism_flag,
        promo.local_flag,
        promo.regional_flag,
        promo.wow_flag,
        promo.no_impacted_stores,
        promo.no_impacted_regions,
        promo.store_format,
        promo.avg_store_size,
        promo.promo_id, 
        promo.promo_year, 
        promo.promo_mechanic, 
        promo.promo_mechanic_description as Promo_mechanic_en,
        promo.name, 
        promo.type, 
        promo.start_date, 
        promo.end_date, 
        promo.class,
        promo.customer_profile_type,
        promo.marketing_type,
        promo.duration, 
        promo.includes_weekend,
        promo.campaign_start_day,
        promo.campaign_start_month,
        promo.campaign_start_quarter,
        promo.campaign_start_week,
        promo.no_to_buy , 
        promo.no_to_pay ,
        promo.discounted_price_promo,
        promo.leaflet_cover,
        promo.leaflet_priv_space,
        promo.in_leaflet_flag,
        promo.in_gondola_flag,
        promo.in_both_leaflet_gondola_flag,
        promo.std_price,
        promo.discount_depth,
        promo.discount_depth_rank,
        promo.promo_weight ,
        bl.promo_flag_binary, 
        bl.change_flag, 
        LAST_VALUE(bl.discount IGNORE NULLS) OVER (ORDER BY sku_root_id, date)/promo_weight as p_discount,
        ttl_sale_amt_bl /promo_weight as p_sale_bl,
        ttl_sale_qty_bl /promo_weight as p_qty_bl,
        ttl_margin_amt_bl /promo_weight as p_margin_bl,
        total_sale_amt /promo_weight as p_sale_amt, 
        total_sale_qty /promo_weight as p_sale_qty,
        total_margin_amt /promo_weight as p_margin_amt,
        ttl_inc_sale_amt /promo_weight as p_ttl_inc_sale_amt,
        ttl_inc_sale_qty /promo_weight as  p_ttl_inc_sale_qty,
        ttl_inc_margin_amt /promo_weight as p_ttl_inc_margin_amt,
        pct_inc_sale,
        pct_inc_qty,
        pct_inc_margin
        FROM promo
        INNER JOIN baseline bl
        USING(sku_root_id, date)
        ;
        """
        
        pareto_section_sku_sql = """



        # Create pareto graph ranking by section and category
        WITH 

        sku_agg AS (
          SELECT sku_root_id, section, category,
          SUM(COALESCE(ttl_inc_margin_amt,0)) as incremental_margin_amt,
          SUM(COALESCE(ttl_inc_sale_amt,0)) as incremental_sale_amt,
          SUM(COALESCE(ttl_inc_sale_qty,0)) as incremental_sale_qty
          FROM `gum-eroski-dev.baseline_performance.baseline_dashboard` 
          where change_flag in (1,2,3)
          GROUP BY sku_root_id, section, category
          ),

        agg_table AS(
        SELECT section as sec_cat, * EXCEPT (section, category) FROM sku_agg
        UNION ALL
        SELECT category as sec_cat, * EXCEPT (section, category) FROM sku_agg),

        sku_margin_rank AS (
          SELECT 
              sec_cat, sku_root_id,
              incremental_margin_amt,
              ROUND(
                  PERCENT_RANK() OVER (
                      PARTITION BY sec_cat
                      ORDER BY incremental_margin_amt), 2) as percentile_rank_inc
              FROM agg_table),
        margin_rank AS(
          SELECT 
          sec_cat,
          percentile_rank_inc,
          SUM(incremental_margin_amt) as incremental_margin_amt,
          ARRAY_AGG(STRUCT(sku_root_id, incremental_margin_amt) ORDER BY sku_root_id) AS margin
          FROM sku_margin_rank
          GROUP BY sec_cat, percentile_rank_inc),

        sku_sale_rank AS (
          SELECT 
              sec_cat, sku_root_id,
              incremental_sale_amt,
              ROUND( PERCENT_RANK() OVER (
                      PARTITION BY sec_cat
                      ORDER BY incremental_sale_amt) ,2) percentile_rank_inc
              FROM agg_table),

        sale_rank AS(
          SELECT 
          sec_cat,
          percentile_rank_inc,
          SUM(incremental_sale_amt) as incremental_sale_amt,
          ARRAY_AGG(STRUCT(sku_root_id, incremental_sale_amt) ORDER BY sku_root_id) AS sale
          FROM sku_sale_rank
          GROUP BY sec_cat, percentile_rank_inc),

        sku_qty_rank AS (
          SELECT 
              sec_cat, sku_root_id,
              incremental_sale_qty,
              ROUND( PERCENT_RANK() OVER (
                      PARTITION BY sec_cat
                      ORDER BY incremental_sale_qty) ,2) percentile_rank_inc
              FROM agg_table),

        qty_rank AS(
          SELECT 
          sec_cat,
          percentile_rank_inc,
          SUM(incremental_sale_qty) as incremental_sale_qty,
          ARRAY_AGG(STRUCT(sku_root_id, incremental_sale_qty) ORDER BY sku_root_id) AS qty
          FROM sku_qty_rank
          GROUP BY sec_cat, percentile_rank_inc)

        Select * 
        FROM margin_rank 
        FULL OUTER JOIN sale_rank
        USING (sec_cat, percentile_rank_inc)
        FULL OUTER JOIN qty_rank
        USING (sec_cat, percentile_rank_inc)

        order by sec_cat, percentile_rank_inc desc;
        
        """
        
        pareto_sku_table_sql = """

        # Create pareto sku table
         WITH 
        sale_amt_rank as (
        SELECT sec_cat, percentile_rank_inc as sale_rank, sale.sku_root_id as sku_root_id, sale.incremental_sale_amt as inc_sale_amt
        FROM `baseline_performance.pareto_section_sku`, UNNEST(sale) as sale), 

        sale_qty_rank as (
        SELECT sec_cat, percentile_rank_inc as qty_rank, qty.sku_root_id as sku_root_id, qty.incremental_sale_qty as inc_sale_qty
        FROM `baseline_performance.pareto_section_sku`, UNNEST(qty) as qty),

        margin_amt_rank as (
        SELECT sec_cat, percentile_rank_inc as margin_rank, margin.sku_root_id as sku_root_id, margin.incremental_margin_amt as inc_margin_amt
        FROM `baseline_performance.pareto_section_sku`, UNNEST(margin) as margin),

        table AS(
        SELECT *
        FROM margin_amt_rank margin
        FULL OUTER JOIN sale_amt_rank sale
        USING (sec_cat, sku_root_id)
        FULL OUTER JOIN sale_qty_rank qty
        USING (sec_cat, sku_root_id))

        SELECT sku_root_id, sku.description, sec_cat, sku.section, sku.category, sku.subcategory, sku.segment, inc_margin_amt, margin_rank, inc_sale_amt, sale_rank,inc_sale_qty , qty_rank 
        FROM table
        LEFT JOIN `ETL.root_sku` sku
        USING (sku_root_id)

        order by section, sku_root_id desc;
        
        """
        
        pareto_promo_sql = """


        # Create promo pareto graph ranking by promo id
         WITH 
        promo_agg AS (
          SELECT promo_id, promo_year,sku_root_id,
          SUM(p_ttl_inc_sale_amt) as inc_sale_amt,
          SUM(p_ttl_inc_sale_qty) as inc_sale_qty,
          SUM(p_ttl_inc_margin_amt) as inc_margin_amt
          FROM `gum-eroski-dev.baseline_performance.baseline_promo`  
          GROUP BY promo_id, promo_year, sku_root_id
          ), 

        promo_margin_rank AS (
          SELECT 
              promo_id, promo_year, sku_root_id,
              inc_margin_amt,
              ROUND(
                  PERCENT_RANK() OVER (
                      PARTITION BY promo_id, promo_year
                      ORDER BY inc_margin_amt), 2) as percentile_rank_inc
              FROM promo_agg),
        margin_rank AS(
          SELECT 
          promo_id, promo_year, 
          percentile_rank_inc,
          SUM(inc_margin_amt) as inc_margin_amt,
          ARRAY_AGG(STRUCT(sku_root_id, inc_margin_amt) ORDER BY sku_root_id) AS margin
          FROM promo_margin_rank
          GROUP BY promo_id, promo_year,percentile_rank_inc),

        promo_sale_rank AS (
          SELECT 
              promo_id, promo_year, sku_root_id,
              inc_sale_amt,
              ROUND(
                  PERCENT_RANK() OVER (
                      PARTITION BY promo_id, promo_year
                      ORDER BY inc_sale_amt), 2) as percentile_rank_inc
              FROM promo_agg),
        sale_rank AS(
          SELECT 
          promo_id, promo_year, 
          percentile_rank_inc,
          SUM(inc_sale_amt) as inc_sale_amt,
          ARRAY_AGG(STRUCT(sku_root_id, inc_sale_amt) ORDER BY sku_root_id) AS sale
          FROM promo_sale_rank
          GROUP BY promo_id, promo_year,percentile_rank_inc),

        promo_qty_rank AS (
          SELECT 
              promo_id, promo_year, sku_root_id,
              inc_sale_qty,
              ROUND(
                  PERCENT_RANK() OVER (
                      PARTITION BY promo_id, promo_year
                      ORDER BY inc_sale_qty), 2) as percentile_rank_inc
              FROM promo_agg),
        qty_rank AS(
          SELECT 
          promo_id, promo_year, 
          percentile_rank_inc,
          SUM(inc_sale_qty) as inc_sale_qty,
          ARRAY_AGG(STRUCT(sku_root_id, inc_sale_qty) ORDER BY sku_root_id) AS qty
          FROM promo_qty_rank
          GROUP BY promo_id, promo_year,percentile_rank_inc)

        SELECT *
        FROM margin_rank 
        FULL OUTER JOIN sale_rank
        USING (promo_id,promo_year, percentile_rank_inc)
        FULL OUTER JOIN qty_rank
        USING (promo_id,promo_year, percentile_rank_inc)

        order by promo_id, promo_year, percentile_rank_inc desc;
        
        """
        
        pareto_promo_table_sql = """

        # Create pareto promo table 
         WITH 
        promo_sale_rank as (
        SELECT promo_id, promo_year, percentile_rank_inc as sale_rank, sale.sku_root_id as sku_root_id, sale.inc_sale_amt as inc_sale_amt
        FROM `baseline_performance.pareto_promo` , UNNEST(sale) as sale), 

        promo_qty_rank as (
        SELECT promo_id, promo_year, percentile_rank_inc as qty_rank, qty.sku_root_id as sku_root_id, qty.inc_sale_qty as inc_sale_qty
        FROM `baseline_performance.pareto_promo` , UNNEST(qty) as qty), 

        promo_margin_rank as (
        SELECT promo_id, promo_year, percentile_rank_inc as margin_rank, margin.sku_root_id as sku_root_id, margin.inc_margin_amt as inc_margin_amt
        FROM `baseline_performance.pareto_promo` , UNNEST(margin) as margin), 

        promo_detail AS (
          SELECT promo_id, promo_year, sku_root_id, name, start_date, end_date, duration,
          SUM(p_discount) as discount
          FROM `gum-eroski-dev.baseline_performance.baseline_promo`
          GROUP BY promo_id, promo_year, sku_root_id, name, start_date, end_date, duration
          ),

        sku_detail AS(
          SELECT sku_root_id , description, area, section, category, subcategory , segment
          FROM `ETL.root_sku`)

        SELECT promo_id, promo_year, name, start_date, end_date, duration, EXTRACT(MONTH FROM start_date) as st_date_month, sku_root_id,description,area, section, category, subcategory , segment, margin_rank, inc_margin_amt, sale_rank, inc_sale_amt, qty_rank, inc_sale_qty, discount
        FROM promo_margin_rank  
        FULL OUTER JOIN promo_sale_rank 
        USING (promo_id, promo_year, sku_root_id)
        FULL OUTER JOIN promo_qty_rank 
        USING (promo_id, promo_year, sku_root_id)
        INNER JOIN promo_detail 
        USING (promo_id, promo_year,sku_root_id)
        INNER JOIN sku_detail 
        USING(sku_root_id)
        ORDER BY promo_id , promo_year, sku_root_id ;

        """
        
        # Create a disctionary to loop over all destination tables and scripts
        baseline_tables = {'baseline_dashboard':baseline_dashboard_sql, 'baseline_promo': promo_dashboard_sql, 
                           'pareto_section_sku': pareto_section_sku_sql, 'pareto_sku_table': pareto_sku_table_sql,
                           'pareto_promo': pareto_promo_sql, 'pareto_promo_table_sql': pareto_promo_table_sql} 
        
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
