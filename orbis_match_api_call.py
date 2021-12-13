
# Import all necessary python and spark packages
import pandas as pd
import pyspark
from typing import List, Dict, Tuple, Any
from datetime import datetime

# Import the OrbisMatchAPIQueryClient class
from src.utils import OrbisMatchAPIQueryClient

# Read in the Orbis API key from the config file
from conf.config import orbis_api_key

# Start the SparkSession
spark = pyspark.sql.SparkSession.builder \
    .appName("OrbisMatchAPIQuery") \
    .getOrCreate()

# Define the main function to be called
def main(spark_session: pyspark.sql.SparkSession, path_to_data: str, company_name_col: str, country_name_col: str, city_name_col: str, orbis_api_key: str, global_id_col: str = None, batch_size: int = 10, max_concurrency: int = 25) -> pd.DataFrame:
    """
    Main function that wraps together all the OrbisMatchAPIQueryClient methods to submit company data to the batch Orbis Match API and returns results as a Pandas DataFrame
    Inputs:
        path_to_data: a string containing the location of the data that should be sent to Orbis Match API
        company_name_col: name of column containing company name data
        country_name_col: name of column containing the country of the company
        city_name_col: name of column containing the city of the company
        orbis_api_key: API key for authorizing calls to Orbis API
        global_id_col: name of column containing the global ID of the company (defaults to None, would expect it to be the BVD ID)
        batch_size: integer value that will determine the number of companies to send to the batch Orbis Match API in one call
        max_concurrency: the maximum number of concurrent threads that will make batch API calls to Orbis
    Returns:
        a Pandas DataFrame with the results of the Orbis Match API calls
    """

    # Give the user a caution message if they set the batch size of the API calls to greater than 10
    if batch_size > 10:
        print(f"CAUTION: The Orbis API tends to fail if batch size is set higher than 10, currently set to {batch_size:,}")

    # Start timing the function
    start_time = datetime.now()

    # Create an instance of the OrbisMatchAPIQueryClient class
    orbis_match_api_query_client = OrbisMatchAPIQueryClient(spark_session=spark, api_key=orbis_api_key)
    
    # Load the input data stored in S3 in parquet format to a Spark DataFrame
    spark_df = orbis_match_api_query_client.load_data_spark_csv(path_to_data=path_to_data)

    # Convert the data loaded into a Spark DataFrame to Pandas
    pandas_df = orbis_match_api_query_client.convert_spark_to_pandas(spark_df=spark_df)
    
    # Chunk the input data into batches corresponding to the number of companies to send to the batch Orbis Match API in one call
    data_chunks_list = orbis_match_api_query_client.chunk_data(df=pandas_df, company_name_col=company_name_col, country_name_col=country_name_col, city_name_col=city_name_col, global_id_col=global_id_col, batch_size=batch_size)

    # Format the company data to be sent to the Orbis API
    match_criteria_tup_list = orbis_match_api_query_client.format_match_query(data_chunks_list=data_chunks_list)

    # Hit the Orbis Match API with the input data with multiple threads, each with a batch of companies
    futures_list = orbis_match_api_query_client.make_concurrent_orbis_match_api_batch_calls(data_batch_nest_tup_list=match_criteria_tup_list, orbis_api_key=orbis_api_key, max_concurrency=max_concurrency)
    
    # Unpack the data returned by the Orbis API, convert to a Pandas DataFrame
    all_matches_df = orbis_match_api_query_client.unpack_futures_orbis_api_results(futures_list=futures_list)

    # Stop timing the function
    end_time = datetime.now()

    # Print the time taken to run the function
    print(f"Time taken to run the function: {end_time - start_time}\n")

    # Check the score of the results
    orbis_match_api_query_client.score_results(results_df=all_matches_df, score_column_name="Score", check_top_n_results_list=[1, 5])

    # Write the data to the desination file path
    orbis_match_api_query_client.write_data_pandas_csv(pandas_df=all_matches_df, path_to_write=f"results/orbis_match_api_results_{end_time.date()}_T_{end_time.time()}.csv")
    
    return all_matches_df


if __name__ == "__main__":

    # Define the path to the input data
    path_to_data = "data/orbis_test_data.csv"

    # Define the column names of the input data
    company_name_col = "name"
    country_name_col = "country"
    city_name_col = "city"
    global_id_col = "bvd_id_number"

    # Define the maximum number of concurrent threads that will be used to make batch API calls to Orbis
    max_concurrency = 50

    # Define the batch size of the API calls
    batch_size = 10

    # Call the main function
    all_matches_df = main(spark_session=spark, path_to_data=path_to_data, company_name_col=company_name_col, country_name_col=country_name_col, city_name_col=city_name_col, orbis_api_key=orbis_api_key, global_id_col=global_id_col, batch_size=batch_size, max_concurrency=max_concurrency)
