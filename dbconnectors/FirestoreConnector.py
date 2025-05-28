from google.cloud import firestore 
from google.cloud.exceptions import NotFound
# import time # No longer used
from .core import DBConnector # Changed import
# from abc import ABC # Removed ABC
import uuid
import pandas as pd # Added import

def create_unique_id():
  """Creates a unique ID using the UUID4 algorithm.

  Returns:
    A string representing a unique ID.
  """
  # Consider using uuid.uuid4() for standard UUID v4
  return str(uuid.uuid1())


class FirestoreConnector(DBConnector): # Removed ABC
    connectorType: str = "Firestore"

    def __init__(self, 
                project_id:str, 
                firestore_database:str):
        """Initializes the Firestore connection and authentication."""
        super().__init__(project_id=project_id, database_name=firestore_database)
        # self.project_id and self.database_name are set by parent
        # It's good practice to use self.database_name if it's intended to be the firestore_database
        self.db = firestore.Client(project=self.project_id, database=self.database_name)

    # --- Methods from DBConnector interface ---

    def retrieve_df(self, query: str) -> pd.DataFrame:
        raise NotImplementedError("FirestoreConnector does not support direct SQL query retrieval into a DataFrame.")

    def getExactMatches(self, query: str) -> str | None:
        raise NotImplementedError("FirestoreConnector does not support getExactMatches.")

    def getSimilarMatches(self, mode: str, user_grouping: str, qe: list, num_matches: int, similarity_threshold: float) -> str | None:
        raise NotImplementedError("FirestoreConnector does not support getSimilarMatches.")

    def make_audit_entry(self, source_type: str, user_grouping: str, model: str, question: str, generated_sql: str, found_in_vector: str, need_rewrite: str, failure_step: str, error_msg: str, full_log_text: str) -> str:
        raise NotImplementedError("FirestoreConnector does not support make_audit_entry.")

    def return_table_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str:
        raise NotImplementedError("FirestoreConnector does not support return_table_schema_sql.")

    def return_column_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str:
        raise NotImplementedError("FirestoreConnector does not support return_column_schema_sql.")

    def test_sql_plan_execution(self, generated_sql: str) -> tuple[bool, str]:
        raise NotImplementedError("FirestoreConnector does not support test_sql_plan_execution.")

    def get_column_samples(self, columns_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("FirestoreConnector does not support get_column_samples.")

    def log_chat(self, session_id: str, user_question: str, generated_sql: str, user_id: str) -> None: # Matched interface
        """Logs a chat message to Firestore.
        Args:
            session_id (str): The ID of the chat session.
            user_question (str): The question the user asked.
            generated_sql (str): The response from the bot (as per interface mapping).
            user_id (str): The ID of the user who sent the message.
        """
        log_entry = { # Renamed variable for clarity
            "session_id": session_id,
            "user_id": user_id, # Used parameter from interface
            "user_question": user_question,
            "bot_response": generated_sql, # Mapped from generated_sql
            "timestamp": firestore.SERVER_TIMESTAMP,
        }
        # Consider adding error handling for the Firestore operation
        self.db.collection("session_logs").document().set(log_entry)  
        
    def get_chat_logs_for_session(self, session_id: str) -> list | None: # Matched interface
        """Gets all chat logs for a given session.
        Args:
            session_id (str): The ID of the chat session.
        Returns:
            A list of chat log dictionaries, or an empty list if no logs are found for the session.
            Compatible with list | None.
        """
        sessions_log_ref = self.db.collection("session_logs")
        query = sessions_log_ref.where(filter=firestore.FieldFilter("session_id", "==", session_id))
        
        docs = query.stream()
        session_history = []
        for doc in docs:
            doc_data = doc.to_dict()
            # Ensure timestamp exists before trying to sort or include it.
            # Firestore SERVER_TIMESTAMP might take a moment to populate after write.
            # If reading immediately after writing, it might be None or not present.
            if doc_data and "timestamp" in doc_data: 
                session_history.append(doc_data)
        
        # Sort by timestamp if present, otherwise, the order might be inconsistent.
        # It's generally better to order by timestamp in the query itself if possible,
        # but Firestore requires an index for that on fields other than document ID.
        # Example: query = sessions_log_ref.where(...).order_by("timestamp")
        # This requires a composite index on (session_id, timestamp).
        try:
            # Attempt to sort only if timestamps are not None
            if all(item.get("timestamp") is not None for item in session_history):
                sorted_session_history = sorted(session_history, key=lambda x: x["timestamp"])
            else:
                # Handle cases where some timestamps might be None if sorting is critical
                # Or accept potentially unsorted if timestamps can be None
                print("Warning: Some chat log entries may have missing timestamps; order may not be guaranteed.")
                sorted_session_history = session_history 
        except TypeError: # Handles comparison issues if timestamps are of mixed types or None
            print("Warning: Could not sort chat log entries by timestamp due to type errors.")
            sorted_session_history = session_history


        # The original return was a list of dicts with specific keys.
        # The interface specifies list | None. An empty list is fine.
        # If an error occurs or truly "None" should be returned, that logic would be added here.
        # For now, returning the (potentially empty) list of dicts.
        # The prompt states current implementation is compatible, implying empty list for no logs.
        
        # Re-structuring to match the original output structure if needed,
        # but the prompt implies the current structure from to_dict() is okay.
        # The original code was:
        # return [{'user_question': item['user_question'], 'bot_response': item['bot_response'],'timestamp':item['timestamp']} for item in sorted_session_history]
        # This assumes 'user_question', 'bot_response', and 'timestamp' are always present.
        
        # To be safe and match the previous structure while ensuring keys exist:
        result_list = []
        for item in sorted_session_history:
            entry = {}
            entry['user_question'] = item.get('user_question')
            entry['bot_response'] = item.get('bot_response')
            entry['timestamp'] = item.get('timestamp')
            # Optionally filter out entries that don't have all required fields, or handle as needed.
            result_list.append(entry)
            
        return result_list

# Ensure a newline at the end of the file
