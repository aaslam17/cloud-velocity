# SQL Validation Agent

This agent validates translated SQL files between a source dialect and BigQuery SQL dialect. It identifies discrepancies and stores the validation output in a BigQuery table.

## Architecture Diagram

![Alt text for the image] (images/arch.png)


## How to Execute

1.  **Set up the environment:**
    *   Install the required Python libraries:
        ```
        pip install -r requirements.txt
        ```
    *   Set the following environment variables in a `.env` file:
        *   `GOOGLE_CLOUD_PROJECT`: Your Google Cloud project ID.
        *   `GOOGLE_CLOUD_LOCATION`: Your Google Cloud location (e.g., `us-central1`).
        *   `VALIDATION_OUTPUT_TABLE`: The BigQuery table where the validation output will be stored (e.g., `your-project.your_dataset.validation_output`).

2.  **Run the agent:**
    *   Move to the translation-validator folder and then run the `adk web` command
        ```
        cd translation-validator
        adk web 
        ```
    *   When the ADK Web console opens, provide the below input details to the agent
        *   `<gcs_source_folder>`: The GCS folder path for the source SQL dialect files.
        *   `<gcs_target_folder>`: The GCS folder path for the target BigQuery SQL dialect files.
        *   `<source_database>`: The name of the source database (e.g., `Teradata`, `Snowflake`, `Oracle`).

## Benefits

*   **Automated validation:** The agent automates the process of validating translated SQL files, saving time and effort.
*   **Accurate discrepancy detection:** The agent uses a powerful language model to accurately identify discrepancies between SQL dialects.
*   **Centralized output:** The validation output is stored in a BigQuery table, making it easy to access and analyze.

## Validation Output

The validation output is stored in the BigQuery table specified in the `VALIDATION_OUTPUT_TABLE` environment variable. The table has the following schema:

*   `Batch_Id`: A unique identifier for each validation run.
*   `Source_File`: The GCS path of the source SQL file.
*   `Target_File`: The GCS path of the target BigQuery SQL file.
*   `Validation_Result`: A JSON object containing the validation results.
*   `Source_Database`: The name of the source database.
*   `Target_Database`: The name of the target database (always `bigquery`).
*   `Insert_Time`: The timestamp when the validation result was inserted into the table.

## Technologies Used

*   [Google ADK](https://cloud.google.com/adk/docs)
*   [Vertex AI](https://cloud.google.com/vertex-ai)
*   [BigQuery](https://cloud.google.com/bigquery)
*   [Cloud Storage](https://cloud.google.com/storage)
