
# Import all necessary python and spark packages
import requests
from ratelimit import limits, sleep_and_retry
import concurrent
from concurrent import futures
import time
from typing import List, Dict, Tuple, Any
import pandas as pd
import pyspark

class OrbisMatchAPIQueryClient():
    """
    Class to query the Orbis Match API with company name, country, and city data
    """

    def __init__(self, spark_session: pyspark.sql.SparkSession, api_key: str, api_url: str = 'https://Orbis.bvdinfo.com/api/orbis/companies/match?QUERY='):
        """
        Initialize the OrbisMatchAPIQueryClient class
        Args:
            spark_session: Spark session object
            api_key: API key for Orbis Match API
            api_url: API endpoint url for Orbis Match API (defaults to 'https://Orbis.bvdinfo.com/api/orbis/companies/match?QUERY=')
        """

        self.spark_session = spark_session
        self.api_key = api_key
        self.api_url = api_url


    def load_data_spark_parquet(self, path_to_data: str) -> pyspark.sql.DataFrame:
        """
        Method to load data from a URL or path string using the Spark parquet format
        Args:
            path_to_data: path to the parquet file (e.g., 's3://bucket/data.parquet')
        Returns:
            Spark DataFrame
        """

        # Load data from URL using Spark parquet format
        spark_df = self.spark_session.read.parquet(path_to_data)

        return spark_df


    def load_data_spark_csv(self, path_to_data: str) -> pyspark.sql.DataFrame:
        """
        Method to load data from a URL or path string using the Spark CSV format
        Args:
            path_to_data: path to the CSV file (e.g., 's3://bucket/data.csv')
        Returns:
            Spark DataFrame
        """

        # Load data from URL using Spark CSV format
        spark_df = self.spark_session.read.csv(path_to_data, header=True, inferSchema=True)

        return spark_df


    def write_data_spark_parquet(self, spark_df: pyspark.sql.DataFrame, path_to_write: str, mode: str = "append") -> None:
        """
        Method to write Spark DataFrame to a given path in parquet format
        Args:
            spark_df: Spark DataFrame
            path_to_write: path to write the parquet file (e.g., 's3://bucket/data.parquet')
            mode: write mode (defaults to 'append')
        Returns:
            None
        """

        # Write Spark DataFrame to 'path_to_write' location in parquet format
        spark_df.write.mode(mode).parquet(path_to_write)

        return None


    def write_data_pandas_csv(self, pandas_df: pd.DataFrame, path_to_write: str, mode: str = "a") -> None:
        """
        Method to write Pandas DataFrame to a given path in csv format
        Args:
            pandas_df: Pandas DataFrame
            path_to_write: path to write the parquet file (e.g., 's3://bucket/data.parquet')
            mode: write mode (defaults to 'a' for append, 'w' for overwrite)
        Returns:
            None
        """

        # Write Pandas DataFrame to 'path_to_write' location in parquet format
        pandas_df.to_csv(path_to_write, mode=mode, index=False)

        return None


    def convert_spark_to_pandas(self, spark_df: pyspark.sql.DataFrame) -> pd.DataFrame:
        """
        Method to convert a Spark DataFrame to a Pandas DataFrame
        Args:
            spark_df: Spark DataFrame
        Returns:
            Pandas DataFrame
        """

        # Convert Spark DataFrame to Pandas DataFrame
        pandas_df = spark_df.toPandas()

        return pandas_df
    

    def convert_pandas_to_spark(self, pandas_df: pd.DataFrame, schema: pyspark.sql.types.StructType = None) -> pyspark.sql.DataFrame:
        """
        Method to convert a Pandas DataFrame to a Spark DataFrame
        Args:
            pandas_df: Pandas DataFrame
            schema: Spark DataFrame schema, expecting a StructType object
        Returns:
            Spark DataFrame
        """
        
        # Convert Pandas DataFrame to Spark DataFrame
        if schema:
            spark_df = self.spark_session.createDataFrame(pandas_df, schema)
        else:
            spark_df = self.spark_session.createDataFrame(pandas_df)

        return spark_df

    
    def chunk_data(self, df: pd.DataFrame, company_name_col: str, country_name_col: str, city_name_col: str, global_id_col: str = None, batch_size: int = 25) -> List[List[Tuple[str, str, str, str]]]:
        """
        Method that will take a Pandas DataFrame containing data on companies and converts data into a list with sublist chunks of size 'batch_size'
        Args:
            df: Pandas DataFrame containing company info
            company_name_col: name of column containing company name data
            country_name_col: name of column containing the country of the company
            city_name_col: name of column containing the city of the company
            global_id_col: name of column containing the global ID of the company (defaults to None, would expect it to be the BVD ID)
            batch_size: integer value that will determine the size of each chunk into which the data will be split
        Returns:
            A list of sublists, each sublists being 'batch_size' items in length, each item containing the company BVD ID (position 0), company name (position 1), country (position 2), and city (position 3)
        """
    
        print(f"Chunking the input data with {df.shape[0]:,} records into batches of size {batch_size:,}")
        
        # Clean company data before reformatting it
        df[company_name_col] = df[company_name_col].fillna("").str.replace("&", "")
        df[country_name_col] = df[country_name_col].fillna("").str.replace("&", "")
        df[city_name_col] = df[city_name_col].fillna("").str.replace("&", "")
        
        # If there is a global ID column also clean it, if not, create one
        if global_id_col:
            df["global_id"] = df[global_id_col].fillna("g_id_null").str.replace("&", "")
        else:
            df["global_id"] = list(range(df.shape[0]))
        
        # Convert company, country, and city name values each to lists
        company_name_list = df[company_name_col].to_list()
        country_name_list = df[country_name_col].to_list()
        city_name_list = df[city_name_col].to_list()
        g_id_list = df["global_id"].to_list()
        
        # Zip together the company, country and city name value lists, storing the zipped tuples in a list
        g_id_company_country_city_list = list(zip(g_id_list, company_name_list, country_name_list, city_name_list))
        
        # Get chunks of records, each sized according to the value of 'batch_size'
        g_id_company_country_city_chunks_list = [g_id_company_country_city_list[x:x+batch_size] for x in range(0, len(g_id_company_country_city_list), batch_size)]
        
        print(f"Input data chunked into {len(g_id_company_country_city_chunks_list):,} batches\n")
        
        return g_id_company_country_city_chunks_list
    

    def format_match_query(self, data_chunks_list: List[List[Tuple[str, str, str, str]]], global_id_pos: int = 0, company_name_pos: int = 1, country_pos: int = 2, city_pos: int = 3) -> List[List[Tuple[str, Dict[str, str]]]]:
        """
        Method that takes a list of chunks of company data and formats it for the Orbis Match API query
        Args:
            company_chunk_list: A list of sublists, each item containing the company name (position 1), country (position 2), and city (position 3)
            company_name_pos: position in the tuples within company_chunk_list that the company name can be found (defaults to 1)
            country_pos: position in the tuples within company_chunk_list that the country data can be found (defaults to 2)
            city_pos: position in the tuples within company_chunk_list that the city data can be found (defaults to 3)
        Returns:
            A list of lists, containing dictionaries, each with one key "Criteria" and one value that is a dictionary containing company data with 'Name', 'Country', and 'City' keys
        """
        
        print("Engineering input data batches to correct format for batch Orbis Match API")

        # Initialize a list that will contain the final results, sublists containing tuples with the company global IDs as well as dictionaries with the company data
        match_criteria_tup_list = []
        
        # For each chunk passed to the function, create a corresponding list containing tuples with the company global IDs as well as dictionaries with the company data
        for chunk in data_chunks_list:
            
            # Intialize a list that will contain several tuples, which will ultimately be a sublist in the final output list
            match_criteria_tup_sublist = []
            
            # For each tuple in each chunk, construct a python tuple containing the company global ID in the first position and a dictionary with the company data in the second position
            for company_tup in chunk:
                match_criteria_tup = (company_tup[global_id_pos], {"Criteria": {"Name": company_tup[company_name_pos], "Country": company_tup[country_pos], "City": company_tup[city_pos]}})
            
                # Append the tuple with company id and data to the sublist
                match_criteria_tup_sublist.append(match_criteria_tup)
            
            # Append the sublist to the overall list
            match_criteria_tup_list.append(match_criteria_tup_sublist)
        
        print("Input data has been restructured\n")
        
        return match_criteria_tup_list
    

    @sleep_and_retry
    def make_orbis_match_api_batch_call(self, data_batch_tup_sublist: List[Tuple[str, Dict[str, str]]], orbis_api_key: str, api_url: str = "https://Orbis.bvdinfo.com/api/orbis/companies/match?QUERY=", select_col_list: list = ["Match.Score", "Match.Name", "Match.MatchedName", "Match.City", "Match.Country", "Match.BvDId"]) -> Tuple[str, Dict[str, Any]]:
        """
        Method to make a batch API call to the Orbis Match API
        Args:
            data_batch_tup_sublist: a list of tuples that contain a global ID in the first position and dictionaries containing company info to send to the Orbis Match API in the second
            orbis_api_key: API key for authorizing calls to Orbis API
            api_url: the base URL of the API to hit (defaults to 'https://Orbis.bvdinfo.com/api/orbis/companies/match?QUERY=')
            select_col_list: a list of columns from the Orbis API to return in the response
        Returns:
            orbis_api_response: the raw payload of the Orbis Match API response
        """
        
        # Instantiate two lists, one to hold each company's global ID value, the other to hold the info for the Orbis Match API query
        company_global_id_list = []
        company_data_batch_sublist = []
        
        # Parse out the company data dictionary from each tuple
        for company_tup in data_batch_tup_sublist:
            
            # Add the company's global ID to the list
            company_global_id_list.append(company_tup[0])

            # Add the company's query data to the list
            company_data_batch_sublist.append(company_tup[1])
        
        # Construct a dictionary containing all the data and criteria for the Orbis API query
        query_dict = {"MATCH":company_data_batch_sublist, "SELECT": select_col_list}

        # Convert the query dictionary into a python str object
        query_str = str(query_dict)

        # Construct the request url by concatenating the url of the API with the query string 
        request_url = api_url + query_str

        # Construct the headers payload containing the API token
        headers_dict = {"ApiToken": orbis_api_key}

        # Make the request to the Orbis Match API and store the response
        orbis_api_response = requests.get(url=request_url, headers=headers_dict)
        
        # Check to make sure the results of the API call are okay
        if orbis_api_response.ok:
            
            # Parse out the batch Orbis Match API response (list of dictionaries)
            orbis_api_response_dict_list = orbis_api_response.json()
            
            # Zip together the global ID list and the API response, create a list of tuples where the global ID is in position 0, the response dictionary is in position 1
            orbis_api_response_g_id_list = list(zip(company_global_id_list, orbis_api_response_dict_list))
        
        else:
            
            # If the API response is bad, then return a list with same structure as if the response was good, but with null values for the API response
            orbis_api_response_g_id_list = zip(company_global_id_list, [None] * len(company_global_id_list))
        
        return orbis_api_response_g_id_list


    def make_concurrent_orbis_match_api_batch_calls(self, data_batch_nest_tup_list: List[List[Tuple[str, Dict[str, str]]]], orbis_api_key: str, api_url: str = "https://Orbis.bvdinfo.com/api/orbis/companies/match?QUERY=", select_col_list: list = ["Match.Score", "Match.Name", "Match.MatchedName", "Match.City", "Match.Country", "Match.BvDId"], max_concurrency: int = 25) -> List[concurrent.futures._base.Future]:
        """
        Method to make several concurrent batch calls to the Orbis Match API
        Args:
            data_batch_nest_tup_list: a list consisting45e of sublists, each containing several dictionaries containing company info to send to the Orbis Match API
            orbis_api_key: API key for authorizing calls to Orbis API
            api_url: the base URL of the API to hit (defaults to 'https://Orbis.bvdinfo.com/api/orbis/companies/match?QUERY=')
            select_col_list: a list of columns from the Orbis API to return in the response
            max_concurrency: the maximum number of concurrent threads that will make batch API calls to Orbis (defaults to 25)
        Returns:
            A list containing futures that have completed execution
        """
        
        # What this function should do:
        # 1. Create as many 'futures' (python threads executing a function) concurrently as is set by the 'max_concurrency' input argument
        # 2. Once/if that threshold is reached, wait until a future is done to add a new future
        # 3. Keep creating futures until the input list is exhausted
        
        # Count the number of sublists in the overall list, as this is how many batch calls total need to be made to the API
        number_of_batch_calls = len(data_batch_nest_tup_list)
        print(f"Making {number_of_batch_calls:,} batch calls to Orbis Match API")
        
        # Use the python "concurrent" module to open multiple pools for executing the Orbis Match API call
        with concurrent.futures.ThreadPoolExecutor() as executor:
            
            # Before looping through each batch API call, initialized two variables to keep track of the futures created and completed, along with a list to store those futures
            # Active futures will be equal to the difference in futures created and futures completed
            futures_created = 0
            futures_completed = 0
            futures_list = []
            
            # While the number of futures created is less than the number of API calls that need to be made, keep looping
            loop_counter = 0
            complete_index_list = []
            while futures_created < number_of_batch_calls:
                
                loop_counter += 1
                print(f"Loop {loop_counter:,}")
                print(f"Futures created: {futures_created:,}")
                print(f"Futures completed before checking: {futures_completed:,}")
                print(f"Active futures before checking: {futures_created - futures_completed:,}")
                
                # Check how many futures have been completed
                for index, future in enumerate(futures_list):
                    if isinstance(future, concurrent.futures._base.Future):
                        if future.done():
                            if index not in complete_index_list:
                                complete_index_list.append(index)
                                futures_completed += 1
                    
                print(f"Futures completed after checking: {futures_completed:,}")
                print(f"Active futures after checking: {futures_created - futures_completed:,}")

                # If there are less futures active than the max concurrency set, then add another future
                if (futures_created - futures_completed) < max_concurrency:
                
                    # Get the sublist from the overall list
                    company_data_batch_tup_sublist = data_batch_nest_tup_list[futures_created]
                    
                    # Create a new future using the thread pool executor
                    new_future = executor.submit(self.make_orbis_match_api_batch_call, data_batch_tup_sublist=company_data_batch_tup_sublist, orbis_api_key=orbis_api_key)
                    
                    # Append that future to the list of futures for storage
                    futures_list.append(new_future)
                    
                    # Increment the number of futures created by 1
                    futures_created += 1

                    print(f"Future instance added to thread pool executor, futures created total: {futures_created:,}\n")

                else:
                    print(f"Max concurrency reached with {futures_created - futures_completed:,} futures active out of {max_concurrency:,} available. Waiting 10 seconds...")
                    time.sleep(10)
                    print("Sleep complete, beginning loop again\n")
                
            print(f"While loop ended, {futures_created:,} futures created, {futures_completed:,} futures completed, waiting on {futures_created - futures_completed:,} active futures to complete...\n")
        
        print(f"Orbis API call function completed\n")
        return futures_list


    def unpack_futures_orbis_api_results(self, futures_list: List[concurrent.futures._base.Future]) -> pd.DataFrame:
        """
        Method that takes a list of 'future' objects from the python 'concurrent' module containing Orbis Match API response data and retrieves their contents, storing them in a Pandas DataFrame
        Args:
            futures_list: list of 'future' objects containing Orbis Match API response data
        Returns:
            a Pandas DataFrame containing all the Orbis API results in relational format
        """
        
        print(f"Unpacking Orbis API results from {len(futures_list):,} different threads")
        
        # For each future in the list of futures retrieve the result and transform the data into _________
        all_df_list = []
        for future in futures_list:
            
            # Retrieve the contents of the future result, which will be a list of tuples with the company global ID in the first position and the Orbis Match API response in a list of dictionaries in the second
            future_result_tup_list = future.result()
            
            # For each tuple in the list, ________
            batch_df_list = []
            for id_result_tup in future_result_tup_list:
                    
                # Parse out the global ID and Orbis Match API response separately
                future_result_global_id = id_result_tup[0]
                future_result_api_response_dict_list = id_result_tup[1]
                
                # Convert the list of dictionaries containing the API response to a Pandas DataFrame
                api_response_df = pd.DataFrame(future_result_api_response_dict_list)
            
                # Add the company global ID to the API response DataFrame
                api_response_df["global_id"] = future_result_global_id
            
                # Store the API response DataFrame for this company in a list
                batch_df_list.append(api_response_df)
                
            # Extend the all DataFrame list with the list of DataFrame from the latest batch
            all_df_list.extend(batch_df_list)
            
        # Concatenate the list of DataFrames together
        all_matches_df = pd.DataFrame()
        for batch_df in all_df_list:
            all_matches_df = pd.concat([all_matches_df, batch_df])
            
        print("Orbis API results converted from raw API response to Pandas DataFrame\n")
            
        return all_matches_df

