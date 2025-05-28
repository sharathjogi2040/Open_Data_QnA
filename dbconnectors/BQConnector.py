"""
BigQuery Connector Class
"""
from google.cloud import bigquery
from google.cloud import bigquery_connection_v1 as bq_connection
from .core import DBConnector # Changed import
from datetime import datetime
import google.auth
import pandas as pd
from google.cloud.exceptions import NotFound

def get_auth_user():
    credentials, project_id = google.auth.default()

    if hasattr(credentials, 'service_account_email'):
        return credentials.service_account_email
    else:
        return "Not Determined"

def bq_specific_data_types(): 
    return '''
    BigQuery offers a wide variety of datatypes to store different types of data effectively. Here's a breakdown of the available categories:
    Numeric Types -
    INTEGER (INT64): Stores whole numbers within the range of -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807. Ideal for non-fractional values.
    FLOAT (FLOAT64): Stores approximate floating-point numbers with a range of -1.7E+308 to 1.7E+308. Suitable for decimals with a degree of imprecision.
    NUMERIC: Stores exact fixed-precision decimal numbers, with up to 38 digits of precision and 9 digits to the right of the decimal point. Useful for precise financial and accounting calculations.
    BIGNUMERIC: Similar to NUMERIC but with even larger scale and precision. Designed for extreme precision in calculations.
    
    Character Types -
    STRING: Stores variable-length Unicode character sequences. Enclosed using single, double, or triple quotes.
    
    Boolean Type -
    BOOLEAN: Stores logical values of TRUE or FALSE (case-insensitive).
    
    Date and Time Types -
    DATE: Stores dates without associated time information.
    TIME: Stores time information independent of a specific date.
    DATETIME: Stores both date and time information (without timezone information).
    TIMESTAMP: Stores an exact moment in time with microsecond precision, including a timezone component for global accuracy.
    
    Other Types
    BYTES: Stores variable-length binary data. Distinguished from strings by using 'B' or 'b' prefix in values.
    GEOGRAPHY: Stores points, lines, and polygons representing locations on the Earth's surface.
    ARRAY: Stores an ordered collection of zero or more elements of the same (non-ARRAY) data type.
    STRUCT: Stores an ordered collection of fields, each with its own name and data type (can be nested).
    
    This list covers the most common datatypes in BigQuery.
    '''


class BQConnector(DBConnector): # Removed ABC
    """
    A connector class for interacting with BigQuery databases.
    Implements the DBConnector interface for BigQuery.
    """

    connectorType: str = "BigQuery" 

    def __init__(self,
                 project_id:str,
                 region:str,
                 opendataqna_dataset:str, # This will be passed as dataset_name to parent
                 audit_log_table_name:str):
        
        super().__init__(project_id=project_id, region=region, dataset_name=opendataqna_dataset)
        # self.project_id, self.region, self.dataset_name are set by parent
        # BQConnector specific attributes:
        self.opendataqna_dataset = opendataqna_dataset # Retained for clarity if used directly
        self.audit_log_table_name = audit_log_table_name
        self.client = self.getconn()

    def getconn(self) -> bigquery.Client: # Added return type hint
        client = bigquery.Client(project=self.project_id)
        return client
    
    def retrieve_df(self, query: str) -> pd.DataFrame: # Matched interface
        return self.client.query_and_wait(query).to_dataframe()

    def make_audit_entry(self, source_type: str, user_grouping: str, model: str, question: str, generated_sql: str, found_in_vector: str, need_rewrite: str, failure_step: str, error_msg: str, full_log_text: str) -> str: # Matched interface (full_log_text)
        auth_user=get_auth_user()
        PROJECT_ID = self.project_id # Use self.project_id from parent

        # Use self.dataset_name from parent for opendataqna_dataset if they are meant to be the same
        # For now, using self.opendataqna_dataset as it's explicitly set and used in BQConnector
        table_id = f"{PROJECT_ID}.{self.opendataqna_dataset}.{self.audit_log_table_name}"
        now = datetime.now()
        client = self.client # Use existing client

        df1 = pd.DataFrame(columns=[
                'source_type', 'project_id', 'user', 'user_grouping', 'model_used',
                'question', 'generated_sql', 'found_in_vector', 'need_rewrite',
                'failure_step', 'error_msg', 'execution_time', 'full_log'
                ])

        new_row = {
                "source_type": source_type, "project_id": str(PROJECT_ID), "user": str(auth_user),
                "user_grouping": user_grouping, "model_used": model, "question": question,
                "generated_sql": generated_sql, "found_in_vector": found_in_vector,
                "need_rewrite": need_rewrite, "failure_step": failure_step, "error_msg": error_msg,
                "execution_time": now, "full_log": full_log_text # Parameter name updated
                }

        df1.loc[len(df1)] = new_row

        db_schema=[
                    bigquery.SchemaField("source_type", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("project_id", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("user", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("user_grouping", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("model_used", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("question", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("generated_sql", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("found_in_vector", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("need_rewrite", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("failure_step", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("error_msg", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("execution_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
                    bigquery.SchemaField("full_log", bigquery.enums.SqlTypeNames.STRING),
                ]

        try:
            client.get_table(table_id)
            table_exists=True
        except NotFound:
            print(f"Table {table_id} is not found. Will create this log table")
            table_exists=False

        if table_exists:
            errors = client.insert_rows_from_dataframe(table=table_id, dataframe=df1, selected_fields=db_schema)
            if not errors or errors == [[]]: # Check for empty list or list of empty lists
                   print("Logged the run")
            else:
                   print(f"Encountered errors while inserting rows: {errors}")
        else:
            job_config = bigquery.LoadJobConfig(schema=db_schema,write_disposition="WRITE_TRUNCATE")
            client.load_table_from_dataframe(df1,table_id,job_config=job_config)
            print(f"Table {table_id} created and data loaded.")
        
        return 'Completed the logging step'

    def create_vertex_connection(self, connection_id : str) -> None: # Added return type hint
        client=bq_connection.ConnectionServiceClient()
        cloud_resource_properties = bq_connection.types.CloudResourceProperties()
        new_connection=bq_connection.Connection(cloud_resource=cloud_resource_properties)
        # Ensure self.region is available from parent __init__
        client.create_connection(parent=f'projects/{self.project_id}/locations/{self.region}',connection=new_connection,connection_id=connection_id)
    
    def create_embedding_model(self, connection_id: str, embedding_model: str) -> None: # Added return type hint
        client = self.client # Use existing client
        # Ensure self.dataset_name (from parent, set to opendataqna_dataset) and self.region are available
        query = f'''CREATE OR REPLACE MODEL `{self.project_id}.{self.dataset_name}.EMBEDDING_MODEL`
                    REMOTE WITH CONNECTION `{self.project_id}.{self.region}.{connection_id}`
                    OPTIONS (ENDPOINT = '{embedding_model}');'''
        client.query_and_wait(query)
   
    def retrieve_matches(self, mode: str, user_grouping: str, qe_str: str, similarity_threshold: float, limit: int) -> list: # qe renamed to qe_str
        """
        This function retrieves the most similar table_schema and column_schema.
        Modes can be either 'table', 'column', or 'example' 
        qe_str: string representation of the embedding list
        """
        matches = []
        # Using self.dataset_name (from parent, set to opendataqna_dataset)
        dataset_path = f'{self.project_id}.{self.dataset_name}'

        if mode == 'table':
            sql = f'''select base.content as tables_content from vector_search(
                 (SELECT * FROM `{dataset_path}.table_details_embeddings` WHERE user_grouping = '{user_grouping}'), "embedding", 
            (SELECT {qe_str} as qe), top_k=> {limit},distance_type=>"COSINE") where 1-distance > {similarity_threshold} '''
        
        elif mode == 'column':
            sql=f'''select base.content as columns_content from vector_search(
                 (SELECT * FROM `{dataset_path}.tablecolumn_details_embeddings` WHERE user_grouping = '{user_grouping}'), "embedding",
            (SELECT {qe_str} as qe), top_k=> {limit}, distance_type=>"COSINE") where 1-distance > {similarity_threshold} '''

        elif mode == 'example': 
            sql=f'''select base.example_user_question, base.example_generated_sql from vector_search ( 
                (SELECT * FROM `{dataset_path}.example_prompt_sql_embeddings` WHERE user_grouping = '{user_grouping}'), "embedding",
            (select {qe_str} as qe), top_k=> {limit}, distance_type=>"COSINE") where 1-distance > {similarity_threshold} '''
    
        else: 
            raise ValueError("No valid mode. Must be either table, column, or example") # Raise error

        results=self.client.query_and_wait(sql).to_dataframe()
        
        if len(results) == 0:
            print(f"Did not find any results for {mode}. Adjust the query parameters.")
            return [] # Return empty list if no results

        print(f"Found {len(results)} similarity matches for {mode}.")
        name_txt = ''
        if mode == 'table': 
            for _ , r in results.iterrows():
                name_txt=name_txt+r["tables_content"]+"\n"
        elif mode == 'column': 
            for _ ,r in results.iterrows():
                name_txt=name_txt+r["columns_content"]+"\n"
        elif mode == 'example': 
            for _ , r in results.iterrows():
                example_user_question=r["example_user_question"]
                example_sql=r["example_generated_sql"]
                name_txt = name_txt + "\n Example_question: "+example_user_question+ "; Example_SQL: "+example_sql
        
        matches.append(name_txt.strip()) # Add stripped text to matches
        return matches

    def getSimilarMatches(self, mode: str, user_grouping: str, qe: list, num_matches: int, similarity_threshold: float) -> str | None: # Matched interface (qe: list)
        qe_str = str(qe) # Convert list to string for SQL query
        
        match_results_list = self.retrieve_matches(mode, user_grouping, qe_str, similarity_threshold, num_matches)
        
        if not match_results_list: # If list is empty
            return None
        return match_results_list[0] # Return the first element, which is the concatenated string

    def getExactMatches(self, query: str) -> str | None: # Matched interface
        # Using self.dataset_name (from parent, set to opendataqna_dataset)
        check_history_sql=f"""SELECT example_user_question,example_generated_sql FROM `{self.project_id}.{self.dataset_name}.example_prompt_sql_embeddings`
                          WHERE lower(example_user_question) = lower("{query}") LIMIT 1; """
        exact_sql_history = self.client.query_and_wait(check_history_sql).to_dataframe()

        if not exact_sql_history.empty:
            # Assuming the first row, first relevant column is the SQL
            final_sql = exact_sql_history["example_generated_sql"].iloc[0]
        else: 
            print("No exact match found for the user prompt")
            final_sql = None
        return final_sql

    def test_sql_plan_execution(self, generated_sql: str) -> tuple[bool, str]: # Matched interface
        try:
            job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            query_job = self.client.query(generated_sql,job_config=job_config)
            exec_result_df = f"This query will process {query_job.total_bytes_processed} bytes."
            correct_sql = True
            print(exec_result_df)
            return correct_sql, exec_result_df
        except Exception as e:
            return False, str(e)

    def return_table_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str: # Matched interface
        user_dataset = f"{self.project_id}.{dataset}"
        table_filter_clause = ""
        if table_names:
            formatted_table_names = [f"'{name}'" for name in table_names]
            table_filter_clause = f"""AND TABLE_NAME IN ({', '.join(formatted_table_names)})"""

        table_schema_sql = f"""
        (SELECT
            TABLE_CATALOG as project_id, TABLE_SCHEMA as table_schema , TABLE_NAME as table_name,  OPTION_VALUE as table_description,
            (SELECT STRING_AGG(column_name, ', ') from `{user_dataset}.INFORMATION_SCHEMA.COLUMNS` where TABLE_NAME= t.TABLE_NAME and TABLE_SCHEMA=t.TABLE_SCHEMA) as table_columns
        FROM
            `{user_dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS` as t
        WHERE
            OPTION_NAME = "description"
            {table_filter_clause}
        ORDER BY
            project_id, table_schema, table_name)
        UNION ALL
        (SELECT
            TABLE_CATALOG as project_id, TABLE_SCHEMA as table_schema , TABLE_NAME as table_name,  "NA" as table_description,
            (SELECT STRING_AGG(column_name, ', ') from `{user_dataset}.INFORMATION_SCHEMA.COLUMNS` where TABLE_NAME= t.TABLE_NAME and TABLE_SCHEMA=t.TABLE_SCHEMA) as table_columns
        FROM
            `{user_dataset}.INFORMATION_SCHEMA.TABLES` as t 
        WHERE 
            NOT EXISTS (SELECT 1 FROM `{user_dataset}.INFORMATION_SCHEMA.TABLE_OPTIONS`  
                        WHERE OPTION_NAME = "description" AND TABLE_NAME= t.TABLE_NAME and TABLE_SCHEMA=t.TABLE_SCHEMA)
            {table_filter_clause}
        ORDER BY
            project_id, table_schema, table_name)
        """
        return table_schema_sql
    
    def return_column_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str: # Matched interface
        user_dataset = f"{self.project_id}.{dataset}"
        table_filter_clause = ""
        if table_names:
            formatted_table_names = [f"'{name}'" for name in table_names]
            table_filter_clause = f"""AND C.TABLE_NAME IN ({', '.join(formatted_table_names)})"""
            
        column_schema_sql = f"""
        SELECT
            C.TABLE_CATALOG as project_id, C.TABLE_SCHEMA as table_schema, C.TABLE_NAME as table_name, C.COLUMN_NAME as column_name,
            C.DATA_TYPE as data_type, C.DESCRIPTION as column_description, 
            CASE 
                WHEN T.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 'This Column is a Primary Key for this table' 
                WHEN T.CONSTRAINT_TYPE = 'FOREIGN KEY' THEN 'This column is Foreign Key' 
                ELSE NULL 
            END as column_constraints
        FROM
            `{user_dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` C 
        LEFT JOIN 
            `{user_dataset}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS` T 
            ON C.TABLE_CATALOG = T.TABLE_CATALOG AND
            C.TABLE_SCHEMA = T.TABLE_SCHEMA AND 
            C.TABLE_NAME = T.TABLE_NAME AND  
            T.ENFORCED ='YES'
        LEFT JOIN 
            `{user_dataset}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE` K
            ON K.CONSTRAINT_NAME=T.CONSTRAINT_NAME AND C.COLUMN_NAME = K.COLUMN_NAME 
        WHERE
            1=1
            {table_filter_clause} 
        ORDER BY
            project_id, table_schema, table_name, column_name;
        """
        return column_schema_sql

    def get_column_samples(self, columns_df: pd.DataFrame) -> pd.DataFrame: # Matched interface
        sample_column_list=[]
        for _ , row in columns_df.iterrows(): # Use _ if index is not needed
            get_column_sample_sql=f'''SELECT STRING_AGG(CAST(value AS STRING)) as sample_values FROM UNNEST((SELECT APPROX_TOP_COUNT({row["column_name"]},5) as osn 
            FROM `{row["project_id"]}.{row["table_schema"]}.{row["table_name"]}`
            ))'''
            column_samples_df=self.retrieve_df(get_column_sample_sql)
            sample_column_list.append(column_samples_df['sample_values'].to_string(index=False))

        columns_df["sample_values"]=sample_column_list
        return columns_df

    # New methods from DBConnector interface
    def log_chat(self, session_id: str, user_question: str, generated_sql: str, user_id: str) -> None:
        raise NotImplementedError("log_chat is not implemented for BQConnector")

    def get_chat_logs_for_session(self, session_id: str) -> list | None:
        raise NotImplementedError("get_chat_logs_for_session is not implemented for BQConnector")

# Ensure a newline at the end of the file
