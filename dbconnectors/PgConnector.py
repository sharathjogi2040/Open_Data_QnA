"""
PostgreSQL Connector Class 
"""
import asyncpg
from google.cloud.sql.connector import Connector as GCloudConnector # Renamed to avoid conflict
from sqlalchemy import create_engine
import pandas as pd 
from sqlalchemy.sql import text
from pgvector.asyncpg import register_vector
import asyncio
from pg8000.exceptions import DatabaseError 

from utilities import root_dir
# from google.cloud.sql.connector import Connector # Duplicate import removed

from .core import DBConnector # Changed import

# Removed ABC as it's not directly inherited
# from abc import ABC 


def pg_specific_data_types(): 
    return '''
    PostgreSQL offers a wide variety of datatypes to store different types of data effectively. Here's a breakdown of the available categories:

    Numeric datatypes -
    SMALLINT: Stores small-range integers between -32768 and 32767.
    INTEGER: Stores typical integers between -2147483648 and 2147483647.
    BIGINT: Stores large-range integers between -9223372036854775808 and 9223372036854775807.
    DECIMAL(p,s): Stores arbitrary precision numbers with a maximum of p digits and s digits to the right of the decimal point.
    NUMERIC: Similar to DECIMAL but with additional features like automatic scaling.
    REAL: Stores single-precision floating-point numbers with an approximate range of -3.4E+38 to 3.4E+38.
    DOUBLE PRECISION: Stores double-precision floating-point numbers with an approximate range of -1.7E+308 to 1.7E+308.


    Character datatypes -
    CHAR(n): Fixed-length character string with a specified length of n characters.
    VARCHAR(n): Variable-length character string with a maximum length of n characters.
    TEXT: Variable-length string with no maximum size limit.
    CHARACTER VARYING(n): Alias for VARCHAR(n).
    CHARACTER: Alias for CHAR.

    Monetary datatypes -
    MONEY: Stores monetary amounts with two decimal places.

    Date/Time datatypes -
    DATE: Stores dates without time information.
    TIME: Stores time of day without date information (optionally with time zone).
    TIMESTAMP: Stores both date and time information (optionally with time zone).
    INTERVAL: Stores time intervals between two points in time.

    Binary types -
    BYTEA: Stores variable-length binary data.
    BIT: Stores single bits.
    BIT VARYING: Stores variable-length bit strings.

    Other types -
    BOOLEAN: Stores true or false values.
    UUID: Stores universally unique identifiers.
    XML: Stores XML data.
    JSON: Stores JSON data.
    ENUM: Stores user-defined enumerated values.
    RANGE: Stores ranges of data values.

    This list covers the most common datatypes in PostgreSQL.
    '''


class PgConnector(DBConnector): # Removed ABC
    """
    A connector class for interacting with PostgreSQL databases.
    Implements the DBConnector interface for PostgreSQL.
    """
    connectorType: str = "PostgreSQL"

    def __init__(self,
                project_id:str, 
                region:str, 
                instance_name:str,
                database_name:str, 
                database_user:str, 
                database_password:str): 
        
        super().__init__(project_id=project_id, 
                         region=region, 
                         instance_name=instance_name,
                         database_name=database_name, 
                         database_user=database_user, 
                         database_password=database_password,
                         dataset_name=None) # dataset_name (schema) is None by default

        self.pool = create_engine(
            "postgresql+pg8000://",
            creator=self.getconn,
        )

    def getconn(self): 
        """
        function to return the database connection object
        """
        # initialize Connector object
        # Renamed import to GCloudConnector to avoid conflict with local Connector class if any
        connector = GCloudConnector() 
        conn = connector.connect(
            f"{self.project_id}:{self.region}:{self.instance_name}",
            "pg8000",
            user=f"{self.database_user}",
            password=f"{self.database_password}",
            db=f"{self.database_name}"
        )
        return conn 

    def retrieve_df(self, query: str) -> pd.DataFrame: # Matched interface
        """ 
        Executes a SQL query and returns the results as a pandas DataFrame.
        Handles potential database errors.
        """
        result_df=pd.DataFrame()
        try: 
            with self.pool.connect() as db_conn:
                df = pd.read_sql_query(text(query), con=db_conn) # pd.read_sql is deprecated, using read_sql_query
                result_df = df
            return result_df
        except Exception as e: 
            print(f"Database Error: {e}")
            # Return an empty DataFrame with an error message column, or raise exception
            return pd.DataFrame({'Error_Message': [str(e)]}) 
        
    async def cache_known_sql(self):
        # Assuming 'scripts' is a subdirectory under root_dir or should be removed
        # For now, keeping it as is, but noting the potential issue.
        df = pd.read_csv(f"{root_dir}/scripts/known_good_sql.csv") 
        df = df.loc[:, ["prompt", "sql", "database_name"]]
        df = df.dropna()

        loop = asyncio.get_running_loop()
        async with GCloudConnector(loop=loop) as connector: # Use aliased GCloudConnector
            conn: asyncpg.Connection = await connector.connect_async(
                f"{self.project_id}:{self.region}:{self.instance_name}", 
                "asyncpg",
                user=f"{self.database_user}",
                password=f"{self.database_password}",
                db=f"{self.database_name}",
            )
            await register_vector(conn)
            await conn.execute("DROP TABLE IF EXISTS query_example_embeddings CASCADE")
            await conn.execute(
                """CREATE TABLE query_example_embeddings(
                                    prompt TEXT,
                                    sql TEXT,
                                    user_grouping TEXT)""" # Assuming user_grouping corresponds to schema or dataset
            )
            tuples = list(df.itertuples(index=False))
            await conn.copy_records_to_table(
                "query_example_embeddings", records=tuples, columns=list(df), timeout=10000
            )
            await conn.close()

    async def retrieve_matches(self, mode: str, user_grouping: str, qe: list, similarity_threshold: float, limit: int) -> list: # Corrected user_groupinguping
        """
        This function retrieves the most similar table_schema and column_schema.
        Modes can be either 'table', 'column', or 'example'.
        user_grouping: Corresponds to schema in PostgreSQL.
        """
        matches = [] 
        loop = asyncio.get_running_loop()
        async with GCloudConnector(loop=loop) as connector: # Use aliased GCloudConnector
            conn: asyncpg.Connection = await connector.connect_async(
                f"{self.project_id}:{self.region}:{self.instance_name}", 
                "asyncpg",
                user=f"{self.database_user}",
                password=f"{self.database_password}",
                db=f"{self.database_name}",
            )
            await register_vector(conn)

            if mode == 'table': 
                sql = """
                    SELECT content as tables_content,
                    1 - (embedding <=> $1) AS similarity
                    FROM table_details_embeddings
                    WHERE 1 - (embedding <=> $1) > $2
                    AND user_grouping = $4 
                    ORDER BY similarity DESC LIMIT $3
                """
            elif mode == 'column': 
                sql = """
                    SELECT content as columns_content,
                    1 - (embedding <=> $1) AS similarity
                    FROM tablecolumn_details_embeddings
                    WHERE 1 - (embedding <=> $1) > $2
                    AND user_grouping = $4
                    ORDER BY similarity DESC LIMIT $3
                """
            elif mode == 'example': 
                sql = """
                    SELECT user_grouping, example_user_question, example_generated_sql,
                    1 - (embedding <=> $1) AS similarity
                    FROM example_prompt_sql_embeddings
                    WHERE 1 - (embedding <=> $1) > $2
                    AND user_grouping = $4 
                    ORDER BY similarity DESC LIMIT $3
                """
            else: 
                raise ValueError("No valid mode. Must be either table, column, or example")
            
            results = await conn.fetch(sql, qe, similarity_threshold, limit, user_grouping)

            if not results: # More Pythonic check for empty list
                print(f"Did not find any results for {mode}. Adjust the query parameters.")
            else:
                print(f"Found {len(results)} similarity matches for {mode}.")

            name_txt = ''
            if mode == 'table': 
                for r in results: name_txt += r["tables_content"]+"\n\n"
            elif mode == 'column': 
                for r in results: name_txt += r["columns_content"]+"\n\n "
            elif mode == 'example': 
                for r in results:
                    name_txt += f"\n Example_question: {r['example_user_question']}; Example_SQL: {r['example_generated_sql']}"
            
            if name_txt: # Add to matches only if something was collected
                matches.append(name_txt.strip())
            await conn.close()
        return matches 

    def getSimilarMatches(self, mode: str, user_grouping: str, qe: list, num_matches: int, similarity_threshold: float) -> str | None: # Matched interface
        # user_grouping is schema for PostgreSQL
        try:
            # Running the async retrieve_matches synchronously
            match_results_list = asyncio.run(self.retrieve_matches(mode=mode, user_grouping=user_grouping, qe=qe, similarity_threshold=similarity_threshold, limit=num_matches))
            if not match_results_list:
                return None
            return match_results_list[0] # Assuming retrieve_matches returns a list with one string element or empty
        except Exception as e:
            print(f"Error in getSimilarMatches: {e}")
            return None


    def test_sql_plan_execution(self, generated_sql: str) -> tuple[bool, str]: # Matched interface
        try:
            # Using EXPLAIN (FORMAT JSON) might be more robust for parsing, but for a string message, ANALYZE is fine.
            sql = f"""EXPLAIN ANALYZE {generated_sql}""" 
            exec_result_df = self.retrieve_df(sql)

            if not exec_result_df.empty:
                if 'Error_Message' in exec_result_df.columns:
                    return False, f"Error executing plan: {exec_result_df['Error_Message'].iloc[0]}"
                else:
                    # Convert DataFrame to a string summary for the message
                    plan_summary = exec_result_df.to_string()
                    return True, f"Execution plan retrieved successfully:\n{plan_summary}"
            else: # Should not happen if query is valid SQL, EXPLAIN returns rows or error
                return False, "Failed to retrieve execution plan (empty result)."
        except Exception as e:
            return False, str(e)
        
    def getExactMatches(self, query: str) -> str | None: # Matched interface
        """ 
        Checks if the exact question is already present in the example SQL set.
        Assumes 'example_prompt_sql_embeddings' table is in the default search_path or schema specified elsewhere.
        """
        # This query implies 'example_prompt_sql_embeddings' table is in a schema included in search_path
        # or should be qualified e.g. self.dataset_name.example_prompt_sql_embeddings if dataset_name is the schema
        # For now, keeping as is, matching interface.
        check_history_sql=f"""SELECT example_user_question,example_generated_sql
        FROM example_prompt_sql_embeddings 
        WHERE lower(example_user_question) = lower('{query}') LIMIT 1; """

        exact_sql_history = self.retrieve_df(check_history_sql)

        if not exact_sql_history.empty and 'Error_Message' not in exact_sql_history.columns:
            # Ensure columns exist before trying to access them
            if "example_generated_sql" in exact_sql_history.columns:
                 return exact_sql_history["example_generated_sql"].iloc[0]
            else:
                print("No 'example_generated_sql' column in results for exact match.")
                return None
        else: 
            if not exact_sql_history.empty and 'Error_Message' in exact_sql_history.columns:
                print(f"Error in getExactMatches SQL: {exact_sql_history['Error_Message'].iloc[0]}")
            else:
                print("No exact match found for the user prompt.")
            return None

    def return_column_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str: # dataset is schema, Matched interface
        table_filter_clause = ""
        if table_names:
            formatted_table_names = [f"'{name}'" for name in table_names]
            table_filter_clause = f"""and c.table_name in ({', '.join(formatted_table_names)})""" # alias c for columns table
            
        column_schema_sql = f'''
        WITH columns_schema AS (
            select 
                c.table_schema, c.table_name, c.column_name, c.data_type, 
                pg_catalog.col_description(c1.oid, c.ordinal_position) as column_description, 
                pg_catalog.obj_description(c1.oid) as table_description
            from information_schema.columns c
            join pg_catalog.pg_class c1 on c.table_name = c1.relname and c.table_schema = pg_catalog.quote_ident(c1.relnamespace::regnamespace::text)
            where c.table_schema = '{dataset}' {table_filter_clause}
        ),
        pk_schema AS (
            SELECT 
                tc.table_name, kcu.column_name AS primary_key
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = '{dataset}' 
                  {table_filter_clause.replace("c.table_name", "tc.table_name")}
        ),
        fk_schema AS (
            SELECT 
                tc.table_name, kcu.column_name AS foreign_key
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = '{dataset}'
                  {table_filter_clause.replace("c.table_name", "tc.table_name")}
        )
        SELECT 
            cs.table_schema, cs.table_name, cs.column_name, cs.data_type, 
            cs.column_description, cs.table_description,
            pk.primary_key,
            CASE 
                WHEN pk.primary_key IS NOT NULL THEN 'Primary key for this table'
                WHEN fk.foreign_key IS NOT NULL THEN 'Foreign key referencing another table' -- Simplified
                ELSE NULL 
            END as column_constraints
        FROM columns_schema cs
        LEFT JOIN pk_schema pk ON cs.table_name = pk.table_name AND cs.column_name = pk.primary_key
        LEFT JOIN fk_schema fk ON cs.table_name = fk.table_name AND cs.column_name = fk.foreign_key
        ORDER BY cs.table_schema, cs.table_name, cs.column_name;
        '''
        return column_schema_sql
    
    def return_table_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str: # dataset is schema, Matched interface
        table_filter_clause = ""
        if table_names:
            formatted_table_names = [f"'{name}'" for name in table_names]
            # Ensure alias 'c' is used if this clause is for a table aliased 'c' or remove alias if not needed
            table_filter_clause = f"""and c.table_name in ({', '.join(formatted_table_names)})"""

        table_schema_sql = f'''
        SELECT 
            c.table_schema, 
            c.table_name, 
            pg_catalog.obj_description(c1.oid) as table_description, 
            array_to_string(array_agg(c.column_name ORDER BY c.ordinal_position), ', ') as table_columns
        FROM information_schema.columns c
        JOIN pg_catalog.pg_class c1 ON c.table_name = c1.relname AND c.table_schema = pg_catalog.quote_ident(c1.relnamespace::regnamespace::text)
        WHERE c.table_schema = '{dataset}' {table_filter_clause}
        GROUP BY c.table_schema, c.table_name, c1.oid
        ORDER BY c.table_name;
        '''
        return table_schema_sql  
    
    def get_column_samples(self, columns_df: pd.DataFrame) -> pd.DataFrame: # Matched interface
        sample_column_list=[]
        for _ , row in columns_df.iterrows(): # Use _ if index is not needed
            # pg_stats might require specific privileges and might not always be up-to-date.
            # Also, most_common_vals is an array, so direct string aggregation might be tricky.
            # This is a simplified approach; more robust parsing of most_common_vals might be needed.
            get_column_sample_sql=f'''SELECT array_to_string(most_common_vals, ', ') AS sample_values 
                                      FROM pg_stats 
                                      WHERE tablename = '{row["table_name"]}' 
                                      AND schemaname = '{row["table_schema"]}' 
                                      AND attname = '{row["column_name"]}' 
                                      AND most_common_vals IS NOT NULL LIMIT 1''' # Add limit and null check

            column_samples_df = self.retrieve_df(get_column_sample_sql)
            if not column_samples_df.empty and 'sample_values' in column_samples_df.columns:
                sample_value = column_samples_df['sample_values'].iloc[0]
                sample_column_list.append(str(sample_value).replace("{","").replace("}","")) # Basic cleaning
            elif not column_samples_df.empty and 'Error_Message' in column_samples_df.columns:
                 sample_column_list.append(f"Error: {column_samples_df['Error_Message'].iloc[0]}")
            else:
                sample_column_list.append(None) # Or "N/A"

        columns_df["sample_values"] = sample_column_list
        return columns_df

    # New methods from DBConnector interface to be implemented
    def make_audit_entry(self, source_type: str, user_grouping: str, model: str, question: str, generated_sql: str, found_in_vector: str, need_rewrite: str, failure_step: str, error_msg: str, full_log_text: str) -> str:
        raise NotImplementedError("Audit logging to PostgreSQL is not implemented yet.")

    def log_chat(self, session_id: str, user_question: str, generated_sql: str, user_id: str) -> None:
        raise NotImplementedError("log_chat is not implemented for PgConnector")

    def get_chat_logs_for_session(self, session_id: str) -> list | None:
        raise NotImplementedError("get_chat_logs_for_session is not implemented for PgConnector")

# Ensure a newline at the end of the file
