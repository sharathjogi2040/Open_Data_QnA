This directory contains documentation and resources to help you understand and use the Open Data QnA library effectively.

## Contents

* **README.md:** This file. Provides an overview of the documentation in this directory.
* **best_practices.md:** Best practices and guidelines for using the library, including recommended configurations, tips for improving performance, and common pitfalls to avoid.
* **faq.md:** Frequently asked questions about the library, covering common issues, troubleshooting tips, and general usage guidance.
* **repo_structure.md:** A detailed explanation of the library's repository structure, including the purpose of each file and directory, and how to navigate the codebase.


## How to Use This Documentation
**Start with the README.md on the root dir:** This file provides a high-level overview and guides you to the relevant resources.
**Consult the FAQ:** If you have any questions or encounter issues, check the FAQ section for possible solutions and answers.
**Explore Best Practices:** For optimizing your usage and getting the most out of the library, review the best practices document.
**Understand the Codebase:** If you want to dive deeper into the library's code, refer to the repository structure document for a detailed explanation of how the code is organized.

### Using JSON Files as a Data Source

The Open Data QnA application now supports using local JSON files as a direct data source. This allows you to ask questions about data contained within these files without needing to load them into a traditional database.

**Specifying a JSON File:**

To use a JSON file, provide its relative or absolute file path (the path must end with `.json`) as the input parameter that normally specifies the database or schema name (e.g., the `user_grouping` argument in `opendataqna.py` or the equivalent in the UI).

**Expected JSON Structure:**

The system is flexible with the JSON structure:

*   **Array of Objects (Ideal):** The most straightforward structure is a JSON array where each element is an object. Each object represents a row, and its keys become the column names. All objects should ideally have consistent keys.
    ```json
    [
      { "id": 1, "name": "Alice", "age": 30 },
      { "id": 2, "name": "Bob", "age": 24 }
    ]
    ```
*   **Single Object with an Array:** If the JSON file contains a single top-level object, the system will look for a key within that object whose value is an array of records as described above.
    ```json
    {
      "results": [
        { "id": 1, "name": "Alice", "age": 30 },
        { "id": 2, "name": "Bob", "age": 24 }
      ]
    }
    ```
*   **Single JSON Object:** A single JSON object (not an array, and not a dictionary containing an array as per the above) will be treated as a single-row table.
    ```json
    { "id": 1, "name": "Alice", "age": 30 }
    ```

**Limitations:**

*   **No Advanced Querying:** The system does not perform SQL-like querying directly on the JSON file. Instead, the entire JSON data is loaded into a pandas DataFrame. Questions are then answered based on the content and structure of this DataFrame.
*   **No Schema Embedding/Similarity Search:** Features related to schema embedding, table/column similarity search, and example query matching are not applicable to JSON data sources in the current implementation. The focus is on direct data retrieval and interpretation.

**Example Usage (Conceptual):**

If you have a local JSON file named `my_project_data.json` in your current working directory, you would typically pass `my_project_data.json` as the data source identifier when prompted by the application or when using the `opendataqna.py` script.
For example:
`python opendataqna.py --user_question "How many records are there?" --user_grouping "my_project_data.json"`
The system will then load `my_project_data.json`, and the LLM will attempt to answer your question based on the data it contains.

### Using Google Sheets as a Data Source

The Open Data QnA application now supports using Google Sheets as a direct data source. This allows you to ask questions about data contained within a Google Sheet.

**Setup Requirements:**

1.  **Enable Google Sheets API**:
    *   In your Google Cloud Project, navigate to "APIs & Services" > "Library".
    *   Search for "Google Sheets API" and ensure it is enabled for your project.
2.  **Create a Service Account**:
    *   In the GCP Console, go to "IAM & Admin" > "Service Accounts".
    *   Click "+ CREATE SERVICE ACCOUNT".
    *   Provide a name and description for the service account.
    *   Grant appropriate project roles if necessary (though for reading sheets, specific roles on the Sheet itself are more critical than project roles).
    *   Click "DONE".
    *   Once created, find the service account in the list, click on it, go to the "KEYS" tab.
    *   Click "ADD KEY" > "Create new key".
    *   Select "JSON" as the key type and click "CREATE". A JSON key file will be downloaded.
3.  **Share Google Sheet**:
    *   Open the Google Sheet you intend to use.
    *   Click the "Share" button (usually top right).
    *   In the "Share with people and groups" dialog, enter the email address of the service account you created (e.g., `your-service-account-name@your-project-id.iam.gserviceaccount.com`). This email is found in the service account's details in GCP or within the downloaded JSON key file (`client_email` field).
    *   Grant the service account at least "Viewer" permissions. "Editor" permissions are not required for read-only access by this application.
    *   Click "Send" or "Share".
4.  **Configure Application**:
    *   **Store Key File**: Place the downloaded service account JSON key file in a secure location accessible by the Open Data QnA application.
    *   **Update `config.ini`**: In your `config.ini` file, add or ensure the following line is present under the `[GCP]` section, pointing to the path of your key file:
        ```ini
        [GCP]
        # ... other GCP settings like PROJECT_ID ...
        GOOGLE_SHEETS_SERVICE_ACCOUNT_KEY_PATH = /path/to/your/service-account-key.json
        ```
        (Replace `/path/to/your/service-account-key.json` with the actual path.)
    *   **Ensure Utility Exposure**: The application expects the `GOOGLE_SHEETS_SERVICE_ACCOUNT_KEY_PATH` to be loaded from `config.ini` and made available via the `utilities` module. If you've recently added this setting, ensure `utilities/__init__.py` exports this variable.

**Specifying a Google Sheet:**

To use a Google Sheet as a data source, you need to provide a specially formatted identifier as the `user_grouping` argument (or its equivalent in the UI). The format is:

`gsheet::SHEET_ID_OR_URL[::WORKSHEET_NAME]`

Where:
*   `gsheet::` is a mandatory prefix.
*   `SHEET_ID_OR_URL`: This can be either:
    *   The full URL of the Google Sheet (e.g., `https://docs.google.com/spreadsheets/d/your_sheet_id/edit`).
    *   Just the Sheet ID itself (e.g., `your_sheet_id` from the URL).
*   `::WORKSHEET_NAME` (optional): This is the specific name of the tab (worksheet) within the spreadsheet you want to query.
    *   If this part is omitted, the application will attempt to read data from the first visible worksheet in the Google Sheet.
    *   The `::` is used as a separator.

**Examples:**

*   To query the first visible worksheet of a sheet with ID `abc123xyz789`:
    `gsheet::abc123xyz789`
*   To query a specific worksheet named `My Data Tab` in the sheet with ID `abc123xyz789`:
    `gsheet::abc123xyz789::My Data Tab`
*   Using the full URL for the same sheet and specific tab:
    `gsheet::https://docs.google.com/spreadsheets/d/abc123xyz789/edit::My Data Tab`

**Current Functionality & Limitations:**

*   **Data Loading**: The connector loads all data from the specified worksheet (or the first visible one if no specific worksheet name is provided) into a pandas DataFrame. The first row of the sheet is typically assumed to be the header row.
*   **Question Answering**: Natural language questions are answered based on the content and structure of this in-memory DataFrame.
*   **No Direct SQL Querying**: The system does not perform SQL-like queries directly against the Google Sheet itself.
*   **No Vector Store Features**: Features related to schema embedding, table/column similarity search, and example query matching (which are available for SQL databases connected to the vector store) are not applicable to Google Sheets sources in the current implementation. The focus is on direct data retrieval and interpretation from the specified sheet.
*   **Read-Only**: The current connector is designed for read-only access.

### Using MBOX Files (Email Archives) as a Data Source

The Open Data QnA application now supports using local MBOX files as a direct data source. MBOX is a common format for storing email archives, such as those exported from Gmail using Google Takeout. This feature allows you to ask questions about the content of your email archives.

**Specifying an MBOX File:**

To use an MBOX file, provide its relative or absolute file path (the path must end with `.mbox`) as the input parameter that normally specifies the database or schema name (e.g., the `user_grouping` argument in `opendataqna.py` or the equivalent in the UI).

*Example:*
To ask questions about a local MBOX file named `my_emails.mbox`, you would typically pass `my_emails.mbox` as the data source identifier.
`python opendataqna.py --user_question "What were the subjects of emails from sender@example.com?" --user_grouping "my_emails.mbox"`

**Data Extraction:**

The MBOX connector parses the MBOX file and extracts key information from each email message. This data is then structured into a table-like format (a pandas DataFrame) where each row represents an email. The following fields are extracted:

*   `From`: The sender's email address.
*   `To`: The recipient(s)' email address(es).
*   `Cc`: The CC recipient(s)' email address(es).
*   `Subject`: The subject line of the email.
*   `Date`: The date and time the email was sent, parsed into a datetime object.
*   `Body`: The plain text content of the email body. The connector attempts to extract the most relevant plain text part from potentially multipart emails.

**Current Functionality & Limitations:**

*   **Data Loading**: The entire MBOX file is parsed, and email data is loaded into an in-memory pandas DataFrame. Natural language questions are answered based on this DataFrame.
*   **No SQL Querying**: The system does not perform SQL-like queries directly on the MBOX file.
*   **Attachment Processing**: Email attachments are not processed, indexed, or included in the extracted data.
*   **Body Extraction**: The body extraction focuses on plain text content. While it attempts to handle multipart emails, complex HTML emails might have their textual representation simplified, and some formatting or non-text content will be lost.
*   **No Vector Store Features**: Features related to schema embedding, table/column similarity search, and example query matching (which are available for SQL databases) are not applicable to MBOX data sources. The system relies on the LLM's ability to understand and answer questions based on the textual content of the emails in the DataFrame.
