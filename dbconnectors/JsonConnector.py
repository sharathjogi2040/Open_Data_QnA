import json
import pandas as pd
from .core import DBConnector # Assuming core.py is in the same directory

class JsonConnector(DBConnector):
    """
    Connector to load data from a JSON file or JSON string.
    Assumes the JSON data represents a list of records (objects with consistent keys)
    that can be directly converted to a pandas DataFrame.
    """
    connectorType = "JSON"

    def __init__(self, 
                 file_path: str | None = None, 
                 json_data: str | dict | list | None = None,
                 project_id: str | None = None, # To match DBConnector, though might not be used
                 region: str | None = None, # To match DBConnector
                 instance_name: str | None = None, # To match DBConnector
                 database_name: str | None = None, # To match DBConnector (can be filename)
                 database_user: str | None = None, # To match DBConnector
                 database_password: str | None = None, # To match DBConnector
                 dataset_name: str | None = None # To match DBConnector
                 ):
        """
        Initializes the JsonConnector.

        Args:
            file_path (str | None): Path to the JSON file.
            json_data (str | dict | list | None): JSON data as a string, dictionary, or list.
                                                 If file_path is provided, this is ignored.
            All other args are to match the DBConnector interface.
        """
        # If the DBConnector in core.py is updated as per the comment in the prompt context, this call would be:
        # super().__init__(project_id=project_id, region=region, instance_name=instance_name,
        #                  database_name=database_name, database_user=database_user,
        #                  database_password=database_password, dataset_name=dataset_name,
        #                  file_path=file_path, json_data=json_data)
        # For now, using the approach where JsonConnector manages these if not in base.
        # The DBConnector definition in the prompt *does* include file_path and json_data.
        super().__init__(project_id=project_id, region=region, instance_name=instance_name,
                         database_name=database_name or file_path, # Use file_path as db_name if db_name not given
                         database_user=database_user, database_password=database_password,
                         dataset_name=dataset_name,
                         file_path=file_path, # Pass to super as per prompt's DBConnector
                         json_data=json_data  # Pass to super as per prompt's DBConnector
                        )
        
        # The DBConnector's __init__ (as per prompt context) now stores self.file_path and self.json_data.
        # JsonConnector can use these directly or manage its own parsed version.
        # The provided code snippet re-assigns self.file_path and creates self.json_data_content.
        # This is fine, it means JsonConnector prefers its own handling logic after super init.

        if self.file_path: # Prioritize file_path (using self.file_path from super)
            self.json_data_content = None # Loaded in retrieve_df
        elif self.json_data is not None: # Use self.json_data from super
            # self.file_path would be None here if only json_data was provided
            if isinstance(self.json_data, str):
                try:
                    self.json_data_content = json.loads(self.json_data)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON string provided: {e}")
            elif isinstance(self.json_data, (dict, list)):
                self.json_data_content = self.json_data # Already parsed
            else:
                raise ValueError("json_data must be a JSON string, dictionary, or list.")
        else:
            # This state implies neither file_path nor json_data was effectively provided to JsonConnector's logic
            raise ValueError("Either file_path or json_data must be provided for JsonConnector.")
        

    def retrieve_df(self, query: str | None = None) -> pd.DataFrame:
        """
        Loads data from the JSON source into a pandas DataFrame.
        The 'query' parameter is currently ignored.
        """
        data_to_process: list | dict | None = None

        if self.file_path: # Uses self.file_path set by __init__ (ultimately from super)
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    data_to_process = json.load(f)
            except FileNotFoundError:
                raise FileNotFoundError(f"JSON file not found: {self.file_path}")
            except json.JSONDecodeError as e:
                raise ValueError(f"Error decoding JSON from file {self.file_path}: {e}")
        elif self.json_data_content is not None:
            data_to_process = self.json_data_content
        else:
            raise ValueError("No JSON data source configured (file_path or json_data_content).")

        # We expect data to be a list of records for direct DataFrame conversion
        if isinstance(data_to_process, list):
            # If it's already a list, use it directly
            pass
        elif isinstance(data_to_process, dict):
            # If it's a dictionary, check if it has a single key whose value is a list of records
            if len(data_to_process) == 1 and isinstance(list(data_to_process.values())[0], list):
                data_to_process = list(data_to_process.values())[0]
            else:
                # If it's a single JSON object (dict not matching the above structure),
                # wrap it in a list to make it a single-row DataFrame.
                data_to_process = [data_to_process]
        else:
            raise ValueError("JSON data must be a list of records or a compatible dictionary structure.")
        
        if not data_to_process: # Handle empty list (e.g. file had '[]' or json_data_content was [])
            return pd.DataFrame()

        return pd.DataFrame(data_to_process)

    # Implement all other abstract methods from DBConnector to raise NotImplementedError
    def getExactMatches(self, query: str) -> str | None:
        raise NotImplementedError("getExactMatches is not implemented for JsonConnector.")

    def getSimilarMatches(self, mode: str, user_grouping: str, qe: list, num_matches: int, similarity_threshold: float) -> str | None:
        raise NotImplementedError("getSimilarMatches is not implemented for JsonConnector.")

    def make_audit_entry(self, source_type: str, user_grouping: str, model: str, question: str, generated_sql: str, found_in_vector: str, need_rewrite: str, failure_step: str, error_msg: str, full_log_text: str) -> str:
        raise NotImplementedError("make_audit_entry is not implemented for JsonConnector.")

    def return_table_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str:
        raise NotImplementedError("return_table_schema_sql is not implemented for JsonConnector.")

    def return_column_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str:
        raise NotImplementedError("return_column_schema_sql is not implemented for JsonConnector.")

    def test_sql_plan_execution(self, generated_sql: str) -> tuple[bool, str]:
        raise NotImplementedError("test_sql_plan_execution is not implemented for JsonConnector.")

    def get_column_samples(self, columns_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("get_column_samples is not implemented for JsonConnector.")

    def log_chat(self, session_id: str, user_question: str, generated_sql: str, user_id: str) -> None:
        raise NotImplementedError("log_chat is not implemented for JsonConnector.")

    def get_chat_logs_for_session(self, session_id: str) -> list | None:
        raise NotImplementedError("get_chat_logs_for_session is not implemented for JsonConnector.")

# Ensure a newline at the end of the file
