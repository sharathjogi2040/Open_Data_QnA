from .core import DBConnector
from .PgConnector import PgConnector, pg_specific_data_types
from .BQConnector import BQConnector, bq_specific_data_types
from .FirestoreConnector import FirestoreConnector
from .JsonConnector import JsonConnector
from .GoogleSheetConnector import GoogleSheetConnector
from .MboxConnector import MboxConnector # Added MboxConnector import
# Configuration values (PROJECT_ID, etc.) are no longer imported directly from utilities.
# They should be passed by the caller of get_connector.

def get_connector(source_type: str, **kwargs) -> DBConnector:
    """
    Factory function to get a database connector instance.

    Args:
        source_type (str): Type of the data source (e.g., 'bigquery', 'postgresql', 'firestore', 'json', 'googlesheet', 'mbox').
        **kwargs: Connector-specific configuration arguments.
                  Expected for 'bigquery': project_id, region, opendataqna_dataset, audit_log_table_name
                  Expected for 'postgresql': project_id, region, instance_name, database_name, database_user, database_password
                  Expected for 'firestore': project_id, firestore_database
                  Expected for 'json': file_path or json_data (and other optional DBConnector args)
                  Expected for 'googlesheet': sheet_id_or_url, credentials_path (and optional worksheet_name, project_id, database_name)
                  Expected for 'mbox': file_path (and other optional DBConnector args)

    Returns:
        DBConnector: An instance of the appropriate DBConnector.

    Raises:
        ValueError: If an unknown source_type is provided or if required arguments are missing.
    """
    source_type_lower = source_type.lower()
    if source_type_lower == 'bigquery':
        required_args = ['project_id', 'region', 'opendataqna_dataset', 'audit_log_table_name']
        missing_args = [k for k in required_args if kwargs.get(k) is None]
        if missing_args:
            raise ValueError(f"Missing required arguments for BigQuery connector: {', '.join(missing_args)}")
        return BQConnector(
            project_id=kwargs['project_id'],
            region=kwargs['region'],
            opendataqna_dataset=kwargs['opendataqna_dataset'],
            audit_log_table_name=kwargs['audit_log_table_name']
        )
    elif source_type_lower == 'postgresql':
        required_args = ['project_id', 'region', 'instance_name', 'database_name', 'database_user', 'database_password']
        missing_args = [k for k in required_args if kwargs.get(k) is None]
        if missing_args:
            raise ValueError(f"Missing required arguments for PostgreSQL connector: {', '.join(missing_args)}")
        return PgConnector(
            project_id=kwargs['project_id'],
            region=kwargs['region'],
            instance_name=kwargs['instance_name'],
            database_name=kwargs['database_name'],
            database_user=kwargs['database_user'],
            database_password=kwargs['database_password']
        )
    elif source_type_lower == 'firestore':
        required_args = ['project_id', 'firestore_database']
        missing_args = [k for k in required_args if kwargs.get(k) is None]
        if missing_args:
            raise ValueError(f"Missing required arguments for Firestore connector: {', '.join(missing_args)}")
        return FirestoreConnector(
            project_id=kwargs['project_id'],
            firestore_database=kwargs['firestore_database']
        )
    elif source_type_lower == 'json':
        if not ('file_path' in kwargs or 'json_data' in kwargs):
            raise ValueError("Missing required arguments for JSON connector: either 'file_path' or 'json_data' must be provided.")
        # JsonConnector can also take DBConnector's standard optional args, so pass them if present
        return JsonConnector(
            file_path=kwargs.get('file_path'),
            json_data=kwargs.get('json_data'),
            project_id=kwargs.get('project_id'), # Optional, for DBConnector base
            region=kwargs.get('region'), # Optional
            instance_name=kwargs.get('instance_name'),
            database_name=kwargs.get('database_name', kwargs.get('file_path')), # Default database_name to file_path
            database_user=kwargs.get('database_user'),
            database_password=kwargs.get('database_password'),
            dataset_name=kwargs.get('dataset_name')
        )
    elif source_type_lower == 'googlesheet':
        required_gs_args = ['sheet_id_or_url', 'credentials_path']
        missing_gs_args = [k for k in required_gs_args if kwargs.get(k) is None]
        if missing_gs_args:
            raise ValueError(f"Missing required arguments for GoogleSheet connector: {', '.join(missing_gs_args)}")
        
        return GoogleSheetConnector(
            sheet_id_or_url=kwargs['sheet_id_or_url'],
            credentials_path=kwargs['credentials_path'],
            worksheet_name=kwargs.get('worksheet_name'), # Optional
            project_id=kwargs.get('project_id'), # Optional, for DBConnector base
            database_name=kwargs.get('database_name', kwargs['sheet_id_or_url']), # Default to sheet_id_or_url
            # Pass other optional DBConnector fields if provided in kwargs
            # These are already passed to GoogleSheetConnector's **kwargs and handled by its super().__init__
            **kwargs # Pass remaining kwargs for DBConnector base class flexibility
        )
    elif source_type_lower == 'mbox':
        required_mbox_args = ['file_path']
        missing_mbox_args = [k for k in required_mbox_args if kwargs.get(k) is None]
        if missing_mbox_args:
            raise ValueError(f"Missing required arguments for Mbox connector: {', '.join(missing_mbox_args)}")
        
        return MboxConnector(
            file_path=kwargs['file_path'],
            project_id=kwargs.get('project_id'), # Optional, for DBConnector base
            database_name=kwargs.get('database_name', kwargs['file_path']), # Default to file_path
            # Pass other optional DBConnector fields if provided in kwargs
            **kwargs # Pass remaining kwargs for DBConnector base class flexibility
        )
    else:
        raise ValueError(f"Unknown source_type: {source_type}")

# Global instances removed.
# Example of old code:
# from utilities import (PROJECT_ID, 
#                        PG_INSTANCE, PG_DATABASE, PG_USER, PG_PASSWORD, PG_REGION,BQ_REGION,
#                        BQ_OPENDATAQNA_DATASET_NAME,BQ_LOG_TABLE_NAME)
# pgconnector = PgConnector(PROJECT_ID, PG_REGION, PG_INSTANCE, PG_DATABASE, PG_USER, PG_PASSWORD)
# bqconnector = BQConnector(PROJECT_ID,BQ_REGION,BQ_OPENDATAQNA_DATASET_NAME,BQ_LOG_TABLE_NAME)
# firestoreconnector = FirestoreConnector(PROJECT_ID,"opendataqna-session-logs")

__all__ = [
    'get_connector', 
    'DBConnector', 
    'PgConnector', 
    'BQConnector', 
    'FirestoreConnector',
    'JsonConnector',
    'GoogleSheetConnector',
    'MboxConnector', # Added MboxConnector to __all__
    'pg_specific_data_types', 
    'bq_specific_data_types'
]
# Ensure a newline at the end of the file