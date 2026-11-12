# STEDI Human Balance Analytics

Data lakehouse project for the STEDI Step Trainer using S3, AWS Glue (PySpark) and Athena.

The data comes from three sources: customer records from the website, accelerometer readings from the mobile app, and readings from the Step Trainer sensor. Each source lands in S3, gets cleaned into a trusted zone, and then gets combined into curated tables. Only customers who agreed to share their data for research end up in the final machine learning table.

S3 bucket used: `stedi-lakehouse-84164`
Glue database: `stedi`

## Files

sql/
- customer_landing.sql
- accelerometer_landing.sql
- step_trainer_landing.sql
- athena_validation_queries.sql (queries I used for the screenshots)

glue_jobs/
- customer_landing_to_trusted.py
- accelerometer_landing_to_trusted.py
- customer_trusted_to_curated.py
- step_trainer_trusted.py
- machine_learning_curated.py

screenshots/
- Athena query results for each table

## Landing zone

The three landing tables were created in Athena with the DDL scripts in the sql folder, pointing at the raw JSON in S3.

- customer_landing: 956 rows
- accelerometer_landing: 81273 rows
- step_trainer_landing: 28680 rows

## Trusted zone

customer_landing_to_trusted.py - keeps only customers with a value in shareWithResearchAsOfDate. Output: customer_trusted (482 rows)

accelerometer_landing_to_trusted.py - inner joins accelerometer_landing with customer_trusted on user = email and keeps only the accelerometer columns. Output: accelerometer_trusted (40981 rows)

step_trainer_trusted.py - inner joins step_trainer_landing with customer_curated on serialNumber and keeps only the step trainer columns. Output: step_trainer_trusted (14460 rows)

## Curated zone

customer_trusted_to_curated.py - inner joins customer_trusted with accelerometer_trusted on email and keeps only the customer columns. SELECT DISTINCT is used so each customer shows up once. Output: customer_curated (482 rows)

machine_learning_curated.py - inner joins step_trainer_trusted with accelerometer_trusted on sensorReadingTime = timestamp. The email column is left out so the table has no PII. Output: machine_learning_curated (43681 rows)

## Notes

- All transforms use the SQL Query node instead of the Join/Filter nodes, since the SQL node gave more consistent results.
- Every job writes to S3 with the option to create a table in the Data Catalog and update the schema on later runs.
- Glue jobs append instead of overwriting, so before rerunning a job I deleted its S3 output folder and dropped the table in Athena.
- Job bookmarks were turned off so reruns read all the data.
