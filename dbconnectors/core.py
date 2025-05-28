"""
Provides the base class for all Connectors 
"""

from abc import ABC, abstractmethod
import pandas as pd

class DBConnector(ABC):
    """
    The core class for all Connectors
    """

    connectorType: str = "Base"

    def __init__(self,
                project_id: str | None = None, 
                region: str | None = None, 
                instance_name: str | None = None,
                database_name: str | None = None, 
                database_user: str | None = None, 
                database_password: str | None = None,
                dataset_name: str | None = None):
        """
        Args:
            project_id (str | None): GCP Project Id.
            region (str | None): GCP Region.
            instance_name (str | None): Instance Name.
            database_name (str | None): Database Name.
            database_user (str | None): Database User.
            database_password (str | None): Database Password.
            dataset_name (str | None): Dataset Name.
        """
        self.project_id = project_id
        self.region = region 
        self.instance_name = instance_name 
        self.database_name = database_name
        self.database_user = database_user
        self.database_password = database_password
        self.dataset_name = dataset_name

    @abstractmethod
    def retrieve_df(self, query: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def getExactMatches(self, query: str) -> str | None:
        pass

    @abstractmethod
    def getSimilarMatches(self, mode: str, user_grouping: str, qe: list, num_matches: int, similarity_threshold: float) -> str | None:
        pass

    @abstractmethod
    def make_audit_entry(self, source_type: str, user_grouping: str, model: str, question: str, generated_sql: str, found_in_vector: str, need_rewrite: str, failure_step: str, error_msg: str, full_log_text: str) -> str:
        pass

    @abstractmethod
    def return_table_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str:
        pass

    @abstractmethod
    def return_column_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str:
        pass

    @abstractmethod
    def test_sql_plan_execution(self, generated_sql: str) -> tuple[bool, str]:
        pass

    @abstractmethod
    def get_column_samples(self, columns_df: pd.DataFrame) -> pd.DataFrame:
        pass

    @abstractmethod
    def log_chat(self, session_id: str, user_question: str, generated_sql: str, user_id: str) -> None:
        pass

    @abstractmethod
    def get_chat_logs_for_session(self, session_id: str) -> list | None:
        pass
