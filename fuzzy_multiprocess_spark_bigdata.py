findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
import time
import pandas as pd
from rapidfuzz import process, fuzz
import multiprocessing

# Configurations
program_name = 'Program XYZ'
output_excel_name = "Matched_Results"
threshold = 90

# Initialize Spark Session
spark = (SparkSession
                .builder
                .appName('StringMatchingDemo')
                .config('spark.dynamicAllocation.enabled', 'false')
                .config('spark.executor.instances', '8')
                .config('spark.executor.cores', '4')
                .config('spark.sql.execution.arrow.pyspark.enabled', True)
                .config('spark.sql.session.timeZone', 'UTC')
                .config('spark.driver.memory', '16G')
                .enableHiveSupport()
                .getOrCreate())

# Function to perform fuzzy matching using rapidfuzz
def get_best_match(name, customer_names):
    return process.extractOne(name, customer_names, scorer=fuzz.ratio)

# Function to match names in parallel using multiprocessing
def match_chunk(chunk, customer_names):
    results = []
    for name in chunk:
        best_match = get_best_match(name, customer_names)
        if best_match:
            matched_name, score, _ = best_match
            matched_customer_id = customer_database_pd[customer_database_pd['customer_name'] == matched_name]['id'].values[0]
            results.append((name, matched_name, score, matched_customer_id))
    return results

# Split data into chunks and parallelize the matching
def parallel_fuzzy_matching(data_to_match, customer_names, num_workers=4):
    chunk_size = len(data_to_match) // num_workers
    chunks = [data_to_match[i:i + chunk_size] for i in range(0, len(data_to_match), chunk_size)]
    with multiprocessing.Pool(processes=num_workers) as pool:
        results = pool.starmap(match_chunk, [(chunk, customer_names) for chunk in chunks])
    flat_results = [item for sublist in results for item in sublist]
    return pd.DataFrame(flat_results, columns=['name_to_match', 'matched_name', 'score', 'matched_customer_id'])

# Dates for data filtering
today_date = (datetime.datetime.today() - datetime.timedelta(days=2)).strftime('%Y%m%d')
as_of_date = (datetime.datetime.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')

# Sample SQL queries to retrieve data (generalized)
data_to_match_df = spark.sql(f"""
SELECT DISTINCT
    created_date,
    customer_id,
    customer_name
FROM data_source_1
WHERE as_of_date = '{today_date}'
    AND program_name = '{program_name}'
""")

customer_database_df = spark.sql(f"""
SELECT customer_id, account_open_date, customer_status, customer_name
FROM data_source_2
WHERE customer_type IN (1, 2, 3)
""")

# Convert Spark DataFrames to Pandas DataFrames
data_to_match_pd = data_to_match_df.toPandas()
customer_database_pd = customer_database_df.toPandas()

# Preprocess the data
data_to_match_pd['customer_name'] = data_to_match_pd['customer_name'].str.upper()
customer_names = customer_database_pd['customer_name'].tolist()

# Start timing
start_time = time.time()

# Perform parallel fuzzy matching
matched_results_df = parallel_fuzzy_matching(data_to_match_pd['customer_name'], customer_names)

# Elapsed time
elapsed_time = time.time() - start_time
print(f"Elapsed time: {elapsed_time:.2f} seconds")

# Join matched results with additional data
realization_df = spark.sql(f"""
SELECT customer_id, account_id, product_name, branch_code, account_open_date
FROM data_source_3
WHERE as_of_date = '{as_of_date}'
""")
realization_pd = realization_df.toPandas()

# Make sure the result is joinned by the id column.
final_results = matched_results_df.merge(realization_pd, 
                                         how='left', 
                                         left_on='matched_customer_id', 
                                         right_on='customer_id')

final_results = final_results[final_results['score'] > threshold]
final_results['account_open_date'] = pd.to_datetime(final_results['account_open_date'])

# Make sure the customer opened their account after the leads program as it counts as KPI.
final_results = final_results[final_results['account_open_date'] >= pd.to_datetime(data_to_match_pd['created_date'].iloc[0])]

# Save the final results to an Excel file
output_path = f'outputs/{output_excel_name}_{today_date}.xlsx'
final_results.to_excel(output_path, index=False)
print(f"Results exported to {output_path}")

print("--- End of Script ---")
