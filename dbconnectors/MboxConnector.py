import mailbox
import pandas as pd
from email.utils import parsedate_to_datetime
from email.message import Message # For type hinting

from .core import DBConnector

class MboxConnector(DBConnector):
    """
    Connector to load data from an MBOX email archive file.
    """
    connectorType = "MBOX"

    def __init__(self,
                 file_path: str,
                 project_id: str | None = None, # For DBConnector base
                 database_name: str | None = None, # For DBConnector base (can be filename)
                 # Add other DBConnector optional params if needed for super()
                 **kwargs # To catch any other DBConnector args passed by factory
                ):
        """
        Initializes the MboxConnector.

        Args:
            file_path (str): Path to the .mbox file.
            project_id (str | None): GCP Project ID (for DBConnector base).
            database_name (str | None): Name for the database (for DBConnector base, can be filename).
        """
        db_name_for_super = database_name or file_path # Use file_path as db_name if not provided
        super().__init__(
            project_id=project_id,
            database_name=db_name_for_super,
            region=kwargs.get('region'),
            instance_name=kwargs.get('instance_name'),
            dataset_name=kwargs.get('dataset_name'),
            database_user=kwargs.get('database_user'),
            database_password=kwargs.get('database_password')
            # As per DBConnector context, file_path is a param it accepts.
            # file_path=file_path # Pass to super
        )
        # MboxConnector uses its own file_path attribute for clarity and direct use.
        self.file_path = file_path

    def _get_email_body(self, message: Message) -> str:
        """Extracts the plain text body from an email message."""
        body = ""
        if message.is_multipart():
            for part in message.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition"))

                if content_type == 'text/plain' and 'attachment' not in content_disposition:
                    try:
                        charset = part.get_content_charset()
                        payload = part.get_payload(decode=True)
                        if charset:
                            body = payload.decode(charset, errors='replace')
                        else:
                            # Try common charsets if not specified
                            try:
                                body = payload.decode('utf-8', errors='replace')
                            except UnicodeDecodeError:
                                body = payload.decode('latin-1', errors='replace')
                        break # Found plain text part
                    except Exception:
                        # Handle cases where decoding might fail or charset is problematic
                        body = "[Could not decode plain text body]"
                        break 
            if not body: # If no explicit text/plain part, try to get first available text
                 for part in message.walk():
                    if part.get_content_type().startswith("text/"):
                        try:
                            charset = part.get_content_charset()
                            payload = part.get_payload(decode=True)
                            if charset: body = payload.decode(charset, errors='replace')
                            else: body = payload.decode('utf-8', errors='replace')
                            break
                        except: pass # ignore if this fallback fails

        else: # Not multipart
            try:
                charset = message.get_content_charset()
                payload = message.get_payload(decode=True)
                if charset:
                    body = payload.decode(charset, errors='replace')
                else:
                    try:
                        body = payload.decode('utf-8', errors='replace')
                    except UnicodeDecodeError:
                        body = payload.decode('latin-1', errors='replace')
            except Exception:
                body = "[Could not decode message body]"
        return body # Removed .strip() as per exact instruction code block

    def retrieve_df(self, query: str | None = None) -> pd.DataFrame:
        """
        Loads data from the MBOX file into a pandas DataFrame.
        Each email is a row, with columns for From, To, Cc, Subject, Date, and Body.
        The 'query' parameter is currently ignored.
        """
        emails_data = []
        try:
            mbox = mailbox.mbox(self.file_path) # Uses self.file_path set in __init__
            for message in mbox:
                try:
                    date_str = message['Date'] # Direct access, less safe than .get()
                    dt_object = None
                    if date_str:
                        try:
                            dt_object = parsedate_to_datetime(date_str)
                        except Exception: # Handle parsing errors
                            dt_object = None # Or keep as string: date_str
                    
                    email_info = {
                        'From': message['From'], # Direct access
                        'To': message['To'],     # Direct access
                        'Cc': message['Cc'],     # Direct access
                        'Subject': message['Subject'], # Direct access
                        'Date': dt_object, # Store as datetime object if parsed
                        'Body': self._get_email_body(message)
                    }
                    emails_data.append(email_info)
                except Exception as e_msg: # More general catch for individual message issues
                    print(f"Error parsing a message in MBOX: {e_msg}")
                    # Optionally append partial data or an error marker
                    # The provided code does not include Message-ID here, so matching that.
                    emails_data.append({'Error': str(e_msg)})


        except FileNotFoundError:
            raise FileNotFoundError(f"MBOX file not found: {self.file_path}")
        except Exception as e: # Catch other mailbox reading errors
            raise RuntimeError(f"Error reading MBOX file {self.file_path}: {e}")

        if not emails_data:
            return pd.DataFrame()
        
        return pd.DataFrame(emails_data)

    # Implement all other abstract methods from DBConnector to raise NotImplementedError
    def getExactMatches(self, query: str) -> str | None:
        raise NotImplementedError("getExactMatches is not implemented for MboxConnector.")

    def getSimilarMatches(self, mode: str, user_grouping: str, qe: list, num_matches: int, similarity_threshold: float) -> str | None:
        raise NotImplementedError("getSimilarMatches is not implemented for MboxConnector.")

    def make_audit_entry(self, source_type: str, user_grouping: str, model: str, question: str, generated_sql: str, found_in_vector: str, need_rewrite: str, failure_step: str, error_msg: str, full_log_text: str) -> str:
        raise NotImplementedError("make_audit_entry is not implemented for MboxConnector.")

    def return_table_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str:
        raise NotImplementedError("return_table_schema_sql is not implemented for MboxConnector.")

    def return_column_schema_sql(self, dataset: str, table_names: list[str] | None = None) -> str:
        raise NotImplementedError("return_column_schema_sql is not implemented for MboxConnector.")

    def test_sql_plan_execution(self, generated_sql: str) -> tuple[bool, str]:
        raise NotImplementedError("test_sql_plan_execution is not implemented for MboxConnector.")

    def get_column_samples(self, columns_df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("get_column_samples is not implemented for MboxConnector.")

    def log_chat(self, session_id: str, user_question: str, generated_sql: str, user_id: str) -> None:
        raise NotImplementedError("log_chat is not implemented for MboxConnector.")

    def get_chat_logs_for_session(self, session_id: str) -> list | None:
        raise NotImplementedError("get_chat_logs_for_session is not implemented for MboxConnector.")

# Ensure a newline at the end of the file
