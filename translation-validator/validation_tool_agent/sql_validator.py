import uuid
from datetime import datetime, timezone
from google.cloud import storage
from google.cloud import bigquery
import vertexai
from urllib.parse import urlparse
from vertexai.generative_models import GenerativeModel, Part
from google.cloud.exceptions import NotFound
from google.adk.tools import FunctionTool
from dotenv import load_dotenv
import os
load_dotenv()

def create_bigquery_table_if_not_exists (full_table_name: str):
  """Creates a BigQuery table if it does not already exist.
  """

  parts = full_table_name.split('.')
  project_id = parts[0]
  dataset_id = parts[1]
  table_id = parts[2]

  client = bigquery.Client(project=project_id)
  table_ref = client.dataset(dataset_id).table(table_id)

  try:
    client.get_table(table_ref)
    print(f"Table {project_id}.{dataset_id}.{table_id} already exists.")
  except NotFound:
    schema = [
        bigquery.SchemaField("Batch_Id", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("Source_File", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("Target_File", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("Validation_Result", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("Source_Database", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("Target_Database", "STRING", mode="NULLABLE", description=""),
        bigquery.SchemaField("Insert_Time", "TIMESTAMP", mode="NULLABLE", description=""),
    ]

    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

def get_validation_prompt(source_file_content, target_file_content):
    validation_prompt=f"""You are an SQL expert specializing in comparing and documenting differences between SQL dialects. 
        Your task is to identify all differences, including syntax, functions, data types, and logic.
 
    Instructions:

    1. Read {source_file_content} and {target_file_content}
    2. Deeply compare the syntax, functions, and data types used in both statements.
    3. Identify only major inconsistencies or differences in the logic between the two statements. 
    4. Ignore the minor inconsistencies or differences and focus only on the major differences


    Output:

    For each identified difference, create a JSON object with the following structure:

    ```json
    {{
    "category": "syntax/function/data_type/logic",
    "teradata_sql": "...",
    "bigquery_sql": "...",
    "explanation": "...",
    "potential_impact": "..."
    }}
    ```

    Where:

    * `"category"` indicates the type of difference (syntax, function, data type, or logic).
    * `"teradata_sql"` shows the specific part of the Teradata SQL related to the difference.
    * `"bigquery_sql"` shows the equivalent part of the BigQuery SQL.
    * `"explanation"` provides a clear description of the difference.
    * `"potential_impact"` describes the potential impact of the difference on the query results or performance.


    Example:
    Input:
    Teradata SQL:
    ```
    SELECT * FROM table1
    WHERE A LIKE ANY ('string1', 'string2');
    ```

    Bigquery SQL:
    ```
    SELECT
        *
    FROM
        table1
    WHERE table1.a LIKE 'string1'
    OR table1.a LIKE 'string2'
    ```
    Output:
    ```
    {{
    "category": "function",
    "teradata_sql": "LIKE ANY",
    "bigquery_sql": "LIKE .. OR",
    "explanation": "'LIKE ANY' function is not available in Bigquery and achieved through LIKE OR Operator",
    "potential_impact": "Validate the functionalities of LIKE ANY is same as LIKE .. OR operator before executing the queries."
    }}
"""
    return validation_prompt    


def run_sql_validation(gcs_source_folder: str, gcs_target_folder: str,source_database: str) -> str:
    """
    Validates translated SQL files between Source Dialect and Bigquery SQL Dialect and store the output of the validation in the bigquery table

    Args:
        gcs_source_folder: Google Cloud Storage folder path of the source files.
        gcs_target_folder: Google Cloud Storage folder path of the target bigquery files.
        source_database: Name of the source database

    Returns:
        A batch id where the results are stored
    """
    try:
        GCP_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
        GCP_LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION")
        validation_output_table = os.environ.get("VALIDATION_OUTPUT_TABLE")

        create_bigquery_table_if_not_exists(validation_output_table)

        storage_client = storage.Client()
        bq_client = bigquery.Client()
        vertexai.init(project=GCP_PROJECT_ID,location=GCP_LOCATION)
        model = GenerativeModel("gemini-2.5-flash-preview-04-17")
        batch_id = str(uuid.uuid4())
        insert_time = datetime.now(timezone.utc)
        errors = []
        successful_comparisons = 0
        target_database="bigquery"

        parsed_source_url = urlparse(gcs_source_folder)
        source_bucket_name = parsed_source_url.netloc
        source_prefix = parsed_source_url.path.lstrip('/')
        if source_prefix and not source_prefix.endswith('/'):
            source_prefix += '/'

        source_bucket = storage_client.bucket(source_bucket_name)
        source_blobs = source_bucket.list_blobs(prefix=source_prefix)
        

        parsed_target_url = urlparse(gcs_target_folder)
        target_bucket_name = parsed_target_url.netloc
        target_prefix = parsed_target_url.path.lstrip('/')
        if target_prefix and not target_prefix.endswith('/'):
            target_prefix += '/'
        target_bucket = storage_client.bucket(target_bucket_name)

        rows_to_insert = []

        for source_blob in source_blobs:
            # Skip if the blob name ends with '/', indicating a directory/prefix
            if source_blob.name.endswith('/'):
                continue

            source_file_name = source_blob.name.split('/')[-1]
            target_blob_name = target_prefix + source_file_name
            target_blob = target_bucket.blob(target_blob_name)
            print(f"Processing source file: {source_blob.name}, target file: {target_blob_name}")

            try:
                source_file_content = source_blob.download_as_text()
                target_file_content = target_blob.download_as_text()
                validation_prompt = get_validation_prompt(source_file_content,target_file_content)
    
                response = model.generate_content([validation_prompt])
                validation_result = response.text
    
                rows_to_insert.append((
                    batch_id,
                    source_blob.name,
                    target_blob.name,
                    validation_result,
                    source_database,
                    target_database,
                    insert_time,
                ))
                successful_comparisons += 1

            except Exception as e:
                errors.append(f"Error processing files {source_blob.name} and {target_blob_name} with Gemini: {e}")

        if rows_to_insert:
            table_id = validation_output_table
            table = bq_client.get_table(table_id)
            bq_client.insert_rows(table, [
                {
                    "Batch_Id": row[0], 
                    "Source_File": row[1],
                    "Target_File": row[2],
                    "Validation_Result": row[3],
                    "Source_Database": row[4],
                    "Target_Database": row[5],
                    "Insert_Time": row[6].isoformat()
                }
                for row in rows_to_insert
            ])
            print(f"Successfully inserted {len(rows_to_insert)} comparison results into BigQuery table: {validation_output_table}")
            

        if errors:
            error_message = "Errors occurred during the process:\n" + "\n".join(errors)
            print(error_message)
            return error_message
        else:
            return batch_id

    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        return error_message
    
#Wrap python function as function tool
run_sql_validation_tool = FunctionTool(run_sql_validation)
