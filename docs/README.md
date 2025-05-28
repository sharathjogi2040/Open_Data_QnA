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
