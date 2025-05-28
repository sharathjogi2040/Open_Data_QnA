import pytest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import json # Added for JsonConnector tests

# Assuming dbconnectors and utilities are importable.
# This might require specific PYTHONPATH setup depending on execution environment.
from dbconnectors import get_connector, DBConnector, BQConnector, PgConnector, FirestoreConnector, JsonConnector # Added JsonConnector
from utilities import (
    PROJECT_ID, BQ_REGION, BQ_OPENDATAQNA_DATASET_NAME, BQ_LOG_TABLE_NAME,
    PG_REGION, PG_INSTANCE, PG_DATABASE, PG_USER, PG_PASSWORD,
)

# Define a default for Firestore DB if not in utilities for testing consistency
try:
    from utilities import FIRESTORE_DATABASE_NAME
except ImportError:
    FIRESTORE_DATABASE_NAME = "opendataqna-test-db-default" # Default for tests

# Sample valid kwargs for each type for convenience
VALID_BQ_KWARGS = {
    "project_id": PROJECT_ID, "region": BQ_REGION,
    "opendataqna_dataset": BQ_OPENDATAQNA_DATASET_NAME,
    "audit_log_table_name": BQ_LOG_TABLE_NAME
}
VALID_PG_KWARGS = {
    "project_id": PROJECT_ID, "region": PG_REGION, "instance_name": PG_INSTANCE,
    "database_name": PG_DATABASE, "database_user": PG_USER, "database_password": PG_PASSWORD
}
VALID_FS_KWARGS = {
    "project_id": PROJECT_ID, "firestore_database": FIRESTORE_DATABASE_NAME
}
VALID_JSON_KWARGS_FILE = {"file_path": "dummy_path.json"}
VALID_JSON_KWARGS_DATA_STR = {"json_data": '[{"colA": 1, "colB": "text"}]'}
VALID_JSON_KWARGS_FULL_FILE = {
    "project_id": PROJECT_ID, "region": "us-central1", "instance_name": "test-instance",
    "database_name": "dummy_path.json", "database_user": None, "database_password": None,
    "dataset_name": None, "file_path": "dummy_path.json", "json_data": None
}


# Patch external calls for all tests in this module if they affect instantiation
@pytest.fixture(autouse=True)
def mock_external_clients():
    """
    Mocks external clients used by the connectors during instantiation.
    This prevents actual network calls and dependencies on external services.
    """
    with patch('google.cloud.bigquery.Client') as mock_bq_client, \
         patch('google.cloud.sql.connector.Connector') as mock_gcloud_sql_connector, \
         patch('google.cloud.firestore.Client') as mock_fs_client, \
         patch('sqlalchemy.create_engine') as mock_create_engine: # For PgConnector's pool

        # Mock BQ Client
        mock_bq_client.return_value = MagicMock()

        # Mock GCloud SQL Connector for PgConnector
        # This mock needs to replicate the behavior of the Connector's connect method
        # and the connection object it returns, specifically for pg8000.
        mock_pg_connection = MagicMock() # This would be the pg8000 connection object
        mock_gcloud_sql_connector_instance = MagicMock()
        mock_gcloud_sql_connector_instance.connect.return_value = mock_pg_connection
        mock_gcloud_sql_connector.return_value = mock_gcloud_sql_connector_instance
        
        # Mock SQLAlchemy engine for PgConnector
        mock_engine_instance = MagicMock()
        mock_engine_instance.connect.return_value.__enter__.return_value = MagicMock() # Mock context manager for "with engine.connect()"
        mock_create_engine.return_value = mock_engine_instance

        # Mock Firestore Client
        mock_fs_client.return_value = MagicMock()
        
        yield mock_bq_client, mock_gcloud_sql_connector, mock_fs_client, mock_create_engine

# --- Tests for get_connector factory ---

def test_get_bq_connector_successful():
    """Test successful creation of BQConnector."""
    connector = get_connector('bigquery', **VALID_BQ_KWARGS)
    assert isinstance(connector, DBConnector)
    assert isinstance(connector, BQConnector)
    assert connector.connectorType == "BigQuery"

def test_get_pg_connector_successful():
    """Test successful creation of PgConnector."""
    connector = get_connector('postgresql', **VALID_PG_KWARGS)
    assert isinstance(connector, DBConnector)
    assert isinstance(connector, PgConnector)
    assert connector.connectorType == "PostgreSQL"

def test_get_fs_connector_successful():
    """Test successful creation of FirestoreConnector."""
    connector = get_connector('firestore', **VALID_FS_KWARGS)
    assert isinstance(connector, DBConnector)
    assert isinstance(connector, FirestoreConnector)
    assert connector.connectorType == "Firestore"

def test_get_connector_unknown_source_type():
    """Test get_connector with an unknown source_type."""
    with pytest.raises(ValueError, match="Unknown source_type: unknown_db"):
        get_connector('unknown_db', **VALID_BQ_KWARGS) # kwargs don't matter here

def test_get_bq_connector_missing_required_arg():
    """Test get_connector for BigQuery with missing arguments."""
    kwargs = VALID_BQ_KWARGS.copy()
    del kwargs['audit_log_table_name']
    with pytest.raises(ValueError, match="Missing required arguments for BigQuery connector: audit_log_table_name"):
        get_connector('bigquery', **kwargs)

def test_get_pg_connector_missing_required_arg():
    """Test get_connector for PostgreSQL with missing arguments."""
    kwargs = VALID_PG_KWARGS.copy()
    del kwargs['database_user']
    with pytest.raises(ValueError, match="Missing required arguments for PostgreSQL connector: database_user"):
        get_connector('postgresql', **kwargs)

def test_get_fs_connector_missing_required_arg():
    """Test get_connector for Firestore with missing arguments."""
    kwargs = VALID_FS_KWARGS.copy()
    del kwargs['firestore_database']
    with pytest.raises(ValueError, match="Missing required arguments for Firestore connector: firestore_database"):
        get_connector('firestore', **kwargs)

# --- Test for basic interface adherence (mocking a method) ---

def test_bq_connector_retrieve_df_mockable(mocker): # pytest-mock's mocker fixture
    """
    Test that a method on a BQConnector instance (created by factory) can be mocked.
    This verifies basic object usability and testability.
    """
    # External client calls during BQConnector instantiation are mocked by mock_external_clients
    connector = get_connector('bigquery', **VALID_BQ_KWARGS)
    
    # Create a dummy DataFrame to be returned by the mocked method
    expected_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    
    # Mock the 'retrieve_df' method on this specific instance
    mocker.patch.object(connector, 'retrieve_df', return_value=expected_df)
    
    # Call the method
    query_string = "SELECT * FROM dummy_table"
    actual_df = connector.retrieve_df(query_string)
    
    # Assertions
    assert actual_df.equals(expected_df)
    connector.retrieve_df.assert_called_once_with(query_string)

def test_pg_connector_methods_raise_not_implemented(mocker):
    """
    Test that PgConnector methods that are not yet fully implemented for all DBConnector features
    (like make_audit_entry) correctly raise NotImplementedError.
    """
    connector = get_connector('postgresql', **VALID_PG_KWARGS)
    with pytest.raises(NotImplementedError, match="Audit logging to PostgreSQL is not implemented yet."):
        connector.make_audit_entry("test_type", "test_group", "test_model", "q", "sql", "N", "N", "step", "err", "log")

def test_fs_connector_methods_raise_not_implemented(mocker):
    """
    Test that FirestoreConnector methods for SQL-like operations
    correctly raise NotImplementedError.
    """
    connector = get_connector('firestore', **VALID_FS_KWARGS)
    with pytest.raises(NotImplementedError, match="FirestoreConnector does not support direct SQL query retrieval into a DataFrame."):
        connector.retrieve_df("ANY SQL")

# --- Tests for JsonConnector in get_connector factory ---

def test_get_json_connector_successful_file_path():
    """Test successful creation of JsonConnector via factory with file_path."""
    connector = get_connector('json', **VALID_JSON_KWARGS_FULL_FILE)
    assert isinstance(connector, DBConnector)
    assert isinstance(connector, JsonConnector)
    assert connector.connectorType == "JSON"
    assert connector.file_path == "dummy_path.json" # Check if file_path is set by super or JsonConnector

def test_get_json_connector_successful_json_data():
    """Test successful creation of JsonConnector via factory with json_data."""
    connector = get_connector('json', json_data='[{"colA": 1}]')
    assert isinstance(connector, DBConnector)
    assert isinstance(connector, JsonConnector)
    assert connector.connectorType == "JSON"
    # Check if json_data is processed and stored (e.g. in json_data_content by JsonConnector)
    # This depends on JsonConnector's __init__ details.
    # Assuming JsonConnector stores parsed data in self.json_data_content
    assert connector.json_data_content == [{"colA": 1}]


def test_get_json_connector_missing_args():
    """Test get_connector for JSON with no file_path or json_data."""
    with pytest.raises(ValueError, match="Missing required arguments for JSON connector: either 'file_path' or 'json_data' must be provided."):
        get_connector('json')

# --- Tests for JsonConnector class itself ---

class TestJsonConnector:

    def test_json_connector_init_with_file_path(self):
        """Test JsonConnector __init__ with file_path."""
        connector = JsonConnector(file_path="test.json")
        assert connector.file_path == "test.json" # Assuming DBConnector.__init__ sets self.file_path
        assert connector.json_data_content is None # Data loaded on demand

    def test_json_connector_init_with_json_data_string(self):
        """Test JsonConnector __init__ with JSON data as a string."""
        json_string = '[{"name": "test"}]'
        connector = JsonConnector(json_data=json_string)
        assert connector.json_data_content == [{"name": "test"}]

    def test_json_connector_init_with_json_data_list(self):
        """Test JsonConnector __init__ with JSON data as a list."""
        json_list = [{"name": "test"}]
        connector = JsonConnector(json_data=json_list)
        assert connector.json_data_content == json_list

    def test_json_connector_init_with_json_data_dict(self):
        """Test JsonConnector __init__ with JSON data as a dictionary."""
        json_dict = {"data": [{"name": "test"}]} # This structure is handled by retrieve_df
        connector = JsonConnector(json_data=json_dict)
        assert connector.json_data_content == json_dict

    def test_json_connector_init_invalid_json_string(self):
        """Test JsonConnector __init__ with an invalid JSON string."""
        with pytest.raises(ValueError, match="Invalid JSON string provided:"):
            JsonConnector(json_data='this is not json')

    def test_json_connector_init_invalid_json_data_type(self):
        """Test JsonConnector __init__ with an invalid type for json_data."""
        with pytest.raises(ValueError, match="json_data must be a JSON string, dictionary, or list."):
            JsonConnector(json_data=12345)

    def test_json_connector_init_no_args(self):
        """Test JsonConnector __init__ with no file_path or json_data."""
        with pytest.raises(ValueError, match="Either file_path or json_data must be provided for JsonConnector."):
            JsonConnector()

    def test_json_connector_retrieve_df_from_file(self, mocker):
        """Test retrieve_df from a mocked file."""
        mock_data = '[{"colA": 1, "colB": "text"}]'
        mocker.patch('builtins.open', mock_open(read_data=mock_data))
        
        connector = JsonConnector(file_path="dummy.json")
        df = connector.retrieve_df()
        
        expected_df = pd.DataFrame([{"colA": 1, "colB": "text"}])
        pd.testing.assert_frame_equal(df, expected_df)

    def test_json_connector_retrieve_df_from_file_not_found(self, mocker):
        """Test retrieve_df with FileNotFoundError."""
        mocker.patch('builtins.open', side_effect=FileNotFoundError("File not found"))
        connector = JsonConnector(file_path="non_existent.json")
        with pytest.raises(FileNotFoundError, match="JSON file not found: non_existent.json"):
            connector.retrieve_df()

    def test_json_connector_retrieve_df_from_file_decode_error(self, mocker):
        """Test retrieve_df with JSONDecodeError."""
        mocker.patch('builtins.open', mock_open(read_data='invalid json {'))
        connector = JsonConnector(file_path="bad_json.json")
        with pytest.raises(ValueError, match="Error decoding JSON from file bad_json.json:"):
            connector.retrieve_df()

    def test_json_connector_retrieve_df_from_json_data_variants(self):
        """Test retrieve_df with various forms of json_data."""
        # Valid JSON string
        connector_str = JsonConnector(json_data='[{"colA": 1, "colB": "X"}]')
        df_str = connector_str.retrieve_df()
        pd.testing.assert_frame_equal(df_str, pd.DataFrame([{"colA": 1, "colB": "X"}]))

        # Valid list
        connector_list = JsonConnector(json_data=[{"colA": 2, "colB": "Y"}])
        df_list = connector_list.retrieve_df()
        pd.testing.assert_frame_equal(df_list, pd.DataFrame([{"colA": 2, "colB": "Y"}]))

        # Valid dict (single list entry)
        connector_dict_list = JsonConnector(json_data={"data": [{"colA": 3, "colB": "Z"}]})
        df_dict_list = connector_dict_list.retrieve_df()
        pd.testing.assert_frame_equal(df_dict_list, pd.DataFrame([{"colA": 3, "colB": "Z"}]))
        
        # Single JSON object (dict)
        connector_single_dict = JsonConnector(json_data={"colA": 4, "colB": "W"})
        df_single_dict = connector_single_dict.retrieve_df()
        pd.testing.assert_frame_equal(df_single_dict, pd.DataFrame([{"colA": 4, "colB": "W"}]))

        # Empty list
        connector_empty_list = JsonConnector(json_data=[])
        df_empty_list = connector_empty_list.retrieve_df()
        assert df_empty_list.empty
        pd.testing.assert_frame_equal(df_empty_list, pd.DataFrame())
        
        # Empty list within a dict
        connector_empty_dict_list = JsonConnector(json_data={"data": []})
        df_empty_dict_list = connector_empty_dict_list.retrieve_df()
        assert df_empty_dict_list.empty
        pd.testing.assert_frame_equal(df_empty_dict_list, pd.DataFrame())


    def test_json_connector_retrieve_df_invalid_json_structure(self):
        """Test retrieve_df with a JSON structure that is not a list or supported dict."""
        # JsonConnector's retrieve_df is designed to handle dicts that are single objects by wrapping them in a list.
        # A dict that *doesn't* represent a list of records and isn't a single object for a row
        # (e.g. multiple top-level keys that are not a single list) is not directly convertible
        # by the current JsonConnector.retrieve_df logic which expects a list or a single object.
        # The current `retrieve_df` wraps any non-list/non-single-key-list-value dict into `[data_to_process]`.
        # So, a dict like {"a": 1, "b": 2} will become pd.DataFrame([{"a": 1, "b": 2}]).
        # This test will verify that behavior.
        connector = JsonConnector(json_data={"key1": "value1", "key2": "value2"})
        df = connector.retrieve_df()
        expected_df = pd.DataFrame([{"key1": "value1", "key2": "value2"}])
        pd.testing.assert_frame_equal(df, expected_df)

        # Test with a type that is neither list nor dict after parsing (should not happen if init is correct)
        # This scenario is more about robustness if json_data_content was somehow set to an invalid type.
        # The current JsonConnector.retrieve_df would raise "JSON data must be a list of records or a compatible dictionary structure."
        # if data_to_process was, e.g., an integer after loading.
        with patch.object(JsonConnector, '__init__', lambda self, json_data: setattr(self, 'json_data_content', 123) or setattr(self, 'file_path', None) ):
            connector_bad_type = JsonConnector(json_data="dummy_ignored") # __init__ is mocked
            with pytest.raises(ValueError, match="JSON data must be a list of records or a compatible dictionary structure."):
                connector_bad_type.retrieve_df()


    def test_json_connector_interface_methods_not_implemented(self):
        """Test that other DBConnector interface methods raise NotImplementedError."""
        connector = JsonConnector(json_data='[]') # Needs valid init
        
        with pytest.raises(NotImplementedError):
            connector.getExactMatches("query")
        with pytest.raises(NotImplementedError):
            connector.getSimilarMatches("mode", "group", [], 1, 0.1)
        with pytest.raises(NotImplementedError):
            connector.make_audit_entry("s_type", "group", "model", "q", "sql", "fv", "nr", "fs", "em", "flt")
        with pytest.raises(NotImplementedError):
            connector.return_table_schema_sql("dataset")
        with pytest.raises(NotImplementedError):
            connector.return_column_schema_sql("dataset")
        with pytest.raises(NotImplementedError):
            connector.test_sql_plan_execution("sql")
        with pytest.raises(NotImplementedError):
            connector.get_column_samples(pd.DataFrame())
        with pytest.raises(NotImplementedError):
            connector.log_chat("sid", "uq", "sql", "uid")
        with pytest.raises(NotImplementedError):
            connector.get_chat_logs_for_session("sid")

# Add a newline at the end of the file
