import pandas as pd
import pandas_gbq

# Project ID
project_id = "gum-eroski-dev"

from google.cloud import bigquery
client = bigquery.Client()
dataset_id = 'WIP'

job_config = bigquery.QueryJobConfig()
# Set the destination table
table_ref = client.dataset(dataset_id).table('calendar_2')
job_config.destination = table_ref
sql = """  SELECT
          day AS date,
          CASE EXTRACT(DAYOFWEEK
          FROM
            day)
            WHEN 1 THEN 'Sunday'
            WHEN 2 THEN 'Monday'
            WHEN 3 THEN 'Tuesday'
            WHEN 4 THEN 'Wednesday'
            WHEN 5 THEN 'Thursday'
            WHEN 6 THEN 'Friday'
            WHEN 7 THEN 'Saturday'
        END
          AS day,
          #extracts week that begins on Monday
          EXTRACT(ISOWEEK
          FROM
            day) AS week,
          CONCAT(CAST(EXTRACT(YEAR
              FROM
                day) AS STRING),'SEM', FORMAT("%02d",(EXTRACT(ISOWEEK
                FROM
                  day)))) AS year_sem,
          CASE EXTRACT(MONTH
          FROM
            day)
            WHEN 1 THEN 'Jan'
            WHEN 2 THEN 'Feb'
            WHEN 3 THEN 'Mar'
            WHEN 4 THEN 'Apr'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'Jun'
            WHEN 7 THEN 'Jul'
            WHEN 8 THEN 'Aug'
            WHEN 9 THEN 'Sept'
            WHEN 10 THEN 'Oct'
            WHEN 11 THEN 'Nov'
            WHEN 12 THEN 'Dec'
        END
          AS month,
          CASE EXTRACT(DAYOFWEEK
          FROM
            day)
            WHEN 1 THEN TRUE
            WHEN 2 THEN FALSE
            WHEN 3 THEN FALSE
            WHEN 4 THEN FALSE
            WHEN 5 THEN FALSE
            WHEN 6 THEN FALSE
            WHEN 7 THEN TRUE
        END
          AS weekend,
          #is it a weekend
          EXTRACT(YEAR
          FROM
            day) AS year,
          EXTRACT(QUARTER
          FROM
            day) AS quarter,
          NULL AS national_holiday_1,
          NULL AS national_holiday_2,
          NULL AS national_holiday_3,
          NULL AS national_event_1,
          NULL AS national_event_2,
          NULL AS national_event_3
        FROM (
          SELECT
            day
          FROM
            UNNEST( GENERATE_DATE_ARRAY(DATE('2017-10-02'), DATE('2019-10-06'), INTERVAL 1 DAY) ) AS day )
        ORDER BY
          date ASC;"""

# Start the query, passing in the extra configuration.
query_job = client.query(
    sql,
    # Location must match that of the dataset(s) referenced in the query
    # and of the destination table.
    location='europe-west3',
    job_config=job_config)  # API request - starts the query

query_job.result()  # Waits for the query to finish
print('Query results loaded to table {}'.format(table_ref.path))


       
#test = pandas_gbq.read_gbq(sql, project_id=project_id) 
#print(test)
