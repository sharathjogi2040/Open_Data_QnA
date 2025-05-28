import pandas as pd
import gspread # type: ignore
# gspread will use google-auth for service account authentication implicitly.

from .core import DBConnector

class GoogleSheetConnector(DBConnector):
    """
    Connector to load data from a Google Sheet.
    """
    connectorType = "GoogleSheet"

    def __init__(self,
                 sheet_id_or_url: str,
                 credentials_path: str, # Path to the service account JSON key file
                 worksheet_name: str | None = None, # Optional: name of the specific worksheet (tab)
                 project_id: str | None = None, # For DBConnector base
                 database_name: str | None = None, # For DBConnector base (can be sheet title)
                 # Add other DBConnector optional params if needed for super()
                 **kwargs # To catch any other DBConnector args passed by factory
                ):
        """
        Initializes the GoogleSheetConnector.

        Args:
            sheet_id_or_url (str): The ID or full URL of the Google Sheet.
            credentials_path (str): Path to the Google Cloud service account JSON key file.
            worksheet_name (str | None): Optional name of the specific worksheet.
                                         If None, the first visible worksheet is used.
            project_id (str | None): GCP Project ID (for DBConnector base).
            database_name (str | None): Name for the database (for DBConnector base, can be sheet title).
        """
        # Determine actual database_name for super() if not provided
        # Potentially extract sheet title later in retrieve_df and assign to self.database_name
        db_name_for_super = database_name or sheet_id_or_url

        super().__init__(
            project_id=project_id,
            database_name=db_name_for_super,
            # Pass other common params from kwargs if they exist in DBConnector.__init__
            region=kwargs.get('region'),
            instance_name=kwargs.get('instance_name'),
            dataset_name=kwargs.get('dataset_name'),
            database_user=kwargs.get('database_user'),
            database_password=kwargs.get('database_password')
            # file_path and json_data from DBConnector are not directly used by GSheet
        )
        
        self.sheet_id_or_url = sheet_id_or_url
        self.credentials_path = credentials_path
        self.worksheet_name = worksheet_name
        self.client: gspread.Client | None = None # Initialized in _connect method

    def _connect(self) -> gspread.Client: # Type hint added
        """Establishes connection to Google Sheets using gspread."""
        if not self.client:
            try:
                self.client = gspread.service_account(filename=self.credentials_path)
            except Exception as e:
                raise ConnectionError(f"Failed to authenticate with Google Sheets using {self.credentials_path}: {e}")
        return self.client

    def retrieve_df(self, query: str | None = None) -> pd.DataFrame:
        """
        Loads data from the specified Google Sheet and worksheet into a pandas DataFrame.
        The 'query' parameter is currently ignored but could be used in the future
        to specify a sheet range (e.g., "A1:D10") or a specific worksheet by name if not given in __init__.
        """
        try:
            client = self._connect()
            
            # Open the sheet by URL or ID
            try:
                if self.sheet_id_or_url.startswith("https://"):
                    sheet = client.open_by_url(self.sheet_id_or_url)
                else:
                    sheet = client.open_by_key(self.sheet_id_or_url) # open_by_key is an alias for ID
            except gspread.exceptions.SpreadsheetNotFound:
                raise FileNotFoundError(f"Google Sheet not found with ID/URL: {self.sheet_id_or_url}")
            except Exception as e: # Catch other gspread opening errors
                raise ConnectionError(f"Failed to open Google Sheet {self.sheet_id_or_url}: {e}")

            # Update self.database_name with actual sheet title if not specifically provided or if it was the ID/URL
            if self.database_name is None or self.database_name == self.sheet_id_or_url:
                self.database_name = sheet.title 

            if self.worksheet_name:
                try:
                    worksheet = sheet.worksheet(self.worksheet_name)
                except gspread.exceptions.WorksheetNotFound:
                    raise ValueError(f"Worksheet '{self.worksheet_name}' not found in sheet '{sheet.title}'.")
            else:
                worksheet = sheet.sheet1 # Default to the first worksheet

            data = worksheet.get_all_records() # Returns a list of dictionaries
            
            if not data: # Handle empty sheet
                return pd.DataFrame()
            
            return pd.DataFrame(data)

        except Exception as e:
            # More specific error handling can be added for API errors, quota limits etc.
            # For now, re-raise a generic exception or return an empty DataFrame with error logged.
            # For consistency with other connectors, let's make it raise an error that can be caught.
            print(f"Error retrieving data from Google Sheet: {e}")
            # Consider if this should return empty df or raise. Raising is more informative.
            raise RuntimeError(f"Error retrieving data from Google Sheet '{self.sheet_id_or_url}': {e}")


    # Implement all other abstract methods from DBConnector to raise NotImplementedError
    def getExactMatches(self, query: str) -> str | None:
        raise NotImplementedError("getExactMatches is not implemented for GoogleSheetConnector.")

    def getSimilarMatches(self, mode: str, user_grouping: str, qe: list, num_matches: int, similarity_threshold: float) -> str | None:
        raise NotImplementedError("getSimilarMatches is not implemented for GoogleSheetConnector.")

    def make_audit_entry(self, source_type: str, user_grouping: str, model: str, question: str, generated_sql: str, found_in_vector: str, need_rewrite: str, failure_step: str, error_msg: str, full_log_text: str) -> str:
        raise NotImplementedError("make_audit_entry is not implemented for GoogleSheetConnector.")

    def return_table_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str:
        raise NotImplementedError("return_table_schema_sql is not implemented for GoogleSheetConnector.")

    def return_column_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str:
        raise NotImplementedError("return_column_schema_sql is not implemented for GoogleSheetConnector.")

    def test_sql_plan_execution(self, generated_sql: str) -> tuple[bool, str]:
        raise NotImplementedError("test_sql_plan_execution is not implemented for GoogleSheetConnector.")

    def get_column_samples(self, columns_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("get_column_samples is not implemented for GoogleSheetConnector.")

    def log_chat(self, session_id: str, user_question: str, generated_sql: str, user_id: str) -> None:
        raise NotImplementedError("log_chat is not implemented for GoogleSheetConnector.")

    def get_chat_logs_for_session(self, session_id: str) -> list | None:
        raise NotImplementedError("get_chat_logs_for_session is not implemented for GoogleSheetConnector.")

# Ensure a newline at the end of the file
