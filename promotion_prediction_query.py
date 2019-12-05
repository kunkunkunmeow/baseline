import pandas as pd
import pandas_gbq
import logging
from google.cloud import bigquery

# Set logger properties
logger = logging.getLogger('promotion_prediction_model')

def promotion_prediction_(project_id, dataset_id, area, mechanic):
        
        # Load client
        client = bigquery.Client()

        job_config = bigquery.QueryJobConfig()
        
        logger.info("Filtering on promotion mechanic {a}...".format(a=mechanic))
        
        promotion_pred_sql = """
         WITH
        temp_aggr_promo AS (
        SELECT
          sku_root_id,
          description,
          area,
          section,
          category,
          subcategory,
          segment,
          brand_name,
          eroskibrand_flag,
          eroskibrand_label,
          wealthy_range_flag,
          flag_healthy,
          innovation_flag,
          tourism_flag,
          local_flag,
          regional_flag,
          no_hipermercados_stores,
          no_supermercados_stores,
          no_gasolineras_stores,
          no_comercio_electronico_stores,
          no_otros_negocio_stores,
          no_plataformas_stores,
          no_other_stores,
          no_impacted_stores,
          no_impacted_regions,
          AVG(avg_store_size) AS avg_store_size,
          promo_id,
          promo_year,
          promo_mechanic,
          promo_mechanic_description as Promo_mechanic_en,
          name,
          type,
          start_date,
          end_date,
          customer_profile_type,
          marketing_type,
          duration AS duration_days,
          MAX(includes_weekend) AS includes_weekend,
          campaign_start_day,
          campaign_start_month,
          campaign_start_quarter,
          campaign_start_week,
          leaflet_cover,
          leaflet_priv_space,
          in_leaflet_flag,
          in_gondola_flag,
          in_both_leaflet_gondola_flag,
          discount_depth,
          discount_depth_rank,
          CASE
            WHEN change_flag in (1,2) THEN 'promotion'
          ELSE
          'post_promotion'
        END
          AS period,
          SUM(tt_discount) AS p_discount,
          SUM(sale_amt_bl) AS p_sale_bl,
          SUM(sale_qty_bl) AS p_qty_bl,
          SUM(margin_amt_bl) AS p_margin_bl,
          SUM(tt_sale_amt) AS p_sale_amt,
          SUM(tt_sale_qty) AS p_sale_qty,
          SUM(tt_margin_amt) AS p_margin_amt,
          SUM(inc_sale_amt) AS p_cal_inc_sale_amt,
          SUM(inc_sale_qty) AS p_cal_inc_sale_qty,
          SUM(inc_margin_amt) AS p_cal_inc_margin_amt,
          SAFE_DIVIDE(SUM(inc_sale_amt),no_impacted_stores) AS p_cal_inc_sale_amt_per_store,
          SAFE_DIVIDE(SUM(inc_sale_qty),no_impacted_stores) AS p_cal_inc_sale_qty_per_store,
          SAFE_DIVIDE(SUM(inc_margin_amt),no_impacted_stores) AS p_cal_inc_margin_amt_per_store,
          SUM(avg_bline_sale) AS p_avg_sale_bl,
          SUM(avg_bline_qty) AS p_avg_qty_bl,
          SUM(avg_bline_margin) AS p_avg_margin_bl,
          SUM(avg_bl_inc_sale) AS p_cal_inc_avg_sale,
          SUM(avg_bl_inc_qty) AS p_cal_inc_avg_qty,
          SUM(avg_bl_inc_margin) AS p_cal_avg_margin,
          SAFE_DIVIDE(SUM(avg_bl_inc_sale),no_impacted_stores) AS p_cal_inc_avg_sale_per_store,
          SAFE_DIVIDE(SUM(avg_bl_inc_qty),no_impacted_stores) AS p_cal_inc_avg_qty_per_store,
          SAFE_DIVIDE(SUM(avg_bl_inc_margin),no_impacted_stores) AS p_cal_avg_margin_per_store
        FROM
          `gum-eroski-dev.baseline.baseline_promo`
        WHERE
          promo_mechanic IN {m}
          AND area = "{a}"
        GROUP BY
          sku_root_id,
          description,
          area,
          section,
          category,
          subcategory,
          segment,
          brand_name,
          eroskibrand_flag,
          eroskibrand_label,
          wealthy_range_flag,
          flag_healthy,
          innovation_flag,
          tourism_flag,
          local_flag,
          regional_flag,
          no_hipermercados_stores,
          no_supermercados_stores,
          no_gasolineras_stores,
          no_comercio_electronico_stores,
          no_otros_negocio_stores,
          no_plataformas_stores,
          no_other_stores,
          no_impacted_stores,
          no_impacted_regions,
          promo_id,
          promo_year,
          promo_mechanic,
          promo_mechanic_description,
          name,
          type,
          start_date,
          end_date,
          customer_profile_type,
          marketing_type,
          duration,
          campaign_start_day,
          campaign_start_month,
          campaign_start_quarter,
          campaign_start_week,
          leaflet_cover,
          leaflet_priv_space,
          in_leaflet_flag,
          in_gondola_flag,
          in_both_leaflet_gondola_flag,
          discount_depth,
          discount_depth_rank,
          period ),
        temp_aggr_promo_f AS (
        SELECT
          * EXCEPT (eroskibrand_flag,
            eroskibrand_label,
            wealthy_range_flag),
          CASE
            WHEN eroskibrand_label IS NOT NULL THEN eroskibrand_label
            WHEN wealthy_range_flag = 'N' THEN 'Normal'
            WHEN wealthy_range_flag = 'S' THEN 'Premium'
          ELSE
          NULL
        END
          AS brand_price_label
        FROM
          temp_aggr_promo )
      SELECT
        *
      FROM
        temp_aggr_promo_f
      WHERE
        discount_depth IS NOT NULL
        AND promo_mechanic IS NOT NULL
        AND sku_root_id IS NOT NULL
        AND segment IS NOT NULL
        AND period IN ('promotion')
      ORDER BY
        sku_root_id,
        promo_id,
        promo_year,
        period
        """.format(m="(\'"+"\',\'".join(str(x) for x in mechanic)+"\')", a=area)
         
        # Create a disctionary to loop over all destination tables and scripts
        tables = {'prediction_train_input':promotion_pred_sql} 
        
        job_config.write_disposition = "WRITE_TRUNCATE"
        for key in tables:
                
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
                
                
def promotion_prediction_res(project_id, dataset_id):
        
        # Load client
        client = bigquery.Client()

        job_config = bigquery.QueryJobConfig()
        
        promo_update = """
        WITH prelim AS 
        (SELECT 
        CAST(pred.p_cal_inc_sale_qty AS NUMERIC) AS p_cal_inc_sale_qty,
        CAST(pred.prediction_interval AS NUMERIC) AS prediction_interval,
        CAST(pred.prediction_error_perc AS NUMERIC) AS prediction_error_perc,
        pred.sku_root_id,pred.description, pred.area, pred.section, pred.category, pred.subcategory, pred.segment,
        pred.brand_name, pred.brand_price_label, pred.flag_healthy, pred.innovation_flag, pred.tourism_flag,
        pred.local_flag, pred.regional_flag, 
        CAST(pred.no_hipermercados_stores AS INT64) AS no_hipermercados_stores,
        CAST(pred.no_supermercados_stores AS INT64) AS no_supermercados_stores,
        CAST(pred.no_gasolineras_stores AS INT64) AS no_gasolineras_stores,
        CAST(pred.no_comercio_electronico_stores AS INT64) AS no_comercio_electronico_stores,
        CAST(pred.no_otros_negocio_stores AS INT64) AS no_otros_negocio_stores,
        CAST(pred.no_plataformas_stores AS INT64) AS no_plataformas_stores,
        CAST(pred.no_other_stores AS INT64) AS no_other_stores,
        CAST(pred.no_impacted_stores AS INT64) AS no_impacted_stores, 
        CAST(pred.no_impacted_regions AS INT64) AS no_impacted_regions,
        CAST(pred.avg_store_size AS NUMERIC) AS avg_store_size,
        CAST(pred.type AS STRING) AS type,
        pred.customer_profile_type,  pred.marketing_type, 
        CAST(pred.duration_days AS INT64) AS duration_days, 
        pred.includes_weekend, pred.campaign_start_day, 
        pred.campaign_start_month , 
        CAST(pred.campaign_start_quarter AS INT64) AS campaign_start_quarter,
        CAST(pred.campaign_start_week AS INT64) AS campaign_start_week, 
        CAST(pred.leaflet_cover AS INT64) AS leaflet_cover,
        CAST(pred.leaflet_priv_space AS INT64) AS leaflet_priv_space, 
        CAST(pred.in_leaflet_flag AS INT64) AS in_leaflet_flag,
        CAST(pred.in_gondola_flag AS INT64) AS in_gondola_flag,
        CAST(pred.in_both_leaflet_gondola_flag AS INT64) AS in_both_leaflet_gondola_flag,
        CAST(pred.p_qty_bl AS NUMERIC) AS p_qty_bl, 
        pred.promo_mechanic, pred.Promo_mechanic_en , pred.discount_depth, 
        CAST(pred.promoted_in_past AS NUMERIC) as promoted_in_past,
        std_price.margin_per_unit as std_margin_per_unit,
        std_price.std_price_per_unit as std_price_per_unit,
        CAST(pred.p_qty_bl AS NUMERIC)*std_price.std_price_per_unit as p_sale_bl,
        CAST(pred.p_qty_bl AS NUMERIC)*std_price.margin_per_unit as p_margin_bl,
        std_price.cost_per_unit as cost_price,
        (CAST(discount_depth_rank AS NUMERIC)/100) as equivalent_discount,
        (1-(CAST(discount_depth_rank AS NUMERIC)/100))*std_price.std_price_per_unit as effective_discount_price_per_unit,
        CAST(pred.p_cal_inc_sale_qty AS NUMERIC)*(1-(CAST(discount_depth_rank AS NUMERIC)/100))*std_price.std_price_per_unit as p_cal_inc_sale_amt,
        (CAST(pred.p_cal_inc_sale_qty AS NUMERIC)*(1-(CAST(discount_depth_rank AS NUMERIC)/100))*std_price.std_price_per_unit) 
        - (CAST(pred.p_cal_inc_sale_qty AS NUMERIC)*(std_price.cost_per_unit)) as p_cal_inc_margin_amt
        FROM `gum-eroski-dev.prediction_results.prediction_promotion_results` pred
        LEFT JOIN `gum-eroski-dev.ETL.aggregate_std_price_margin` std_price
        on std_price.sku_root_id = pred.sku_root_id
        ) 
        SELECT 
        *, 
        SAFE_DIVIDE(p_cal_inc_sale_qty, p_qty_bl) AS perc_uplift_qty,
        SAFE_DIVIDE(p_cal_inc_sale_amt, p_sale_bl) AS perc_uplift_amt,
        SAFE_DIVIDE(p_cal_inc_margin_amt, p_margin_bl) AS perc_uplift_margin
        FROM prelim pred
        """

        promotion_pred_sql = """
        SELECT avg(p_cal_inc_sale_qty) as avg_p_cal_inc_sale_qty, 
        avg(p_cal_inc_sale_amt) as avg_p_cal_inc_sale_amt,
        avg(p_cal_inc_margin_amt) as avg_p_cal_inc_margin_amt,

        avg(perc_uplift_qty) as avg_perc_uplift_qty,
        avg(perc_uplift_amt) as avg_perc_uplift_amt,
        avg(perc_uplift_margin) as avg_perc_uplift_margin,

        sum(p_cal_inc_sale_qty) as sum_p_cal_inc_sale_qty, 
        sum(p_cal_inc_sale_amt) as sum_p_cal_inc_sale_amt,
        sum(p_cal_inc_margin_amt) as sum_p_cal_inc_margin_amt,

        avg(prediction_interval) as avg_prediction_interval,
        avg(prediction_error_perc) as avg_prediction_error_perc,
        area, section, category, subcategory, brand_name, 
        promo_mechanic, Promo_mechanic_en, discount_depth, 
        count(distinct sku_root_id) as no_skus_in_brand_cat,
        max(promoted_in_past) as promoted_in_past

        FROM `gum-eroski-dev.prediction_results.prediction_promotion_results`

        group by area, section, category, subcategory, brand_name, promo_mechanic, Promo_mechanic_en, discount_depth
        """ 
         
        # Create a disctionary to loop over all destination tables and scripts
        tables = {'prediction_promotion_results':promo_update,
                  'prediction_promotion_results_cat_brand':promotion_pred_sql} 
        
        job_config.write_disposition = "WRITE_TRUNCATE"
        for key in tables:
                
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

