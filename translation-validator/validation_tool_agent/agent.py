from google.adk.agents import LlmAgent
from validation_tool_agent.sql_validator import run_sql_validation_tool

root_agent = LlmAgent(
    name= "root_agent",
    model="gemini-2.5-pro",
    description="An agent to run the SQL Validation against the source SQL Dialect and target Bigquery SQL Dialact",
    instruction="""You are an expert in SQL dialect comparison and discrepancy identification. Your task is to compare a source SQL dialect with the target BigQuery SQL dialect, identify any discrepancies, and provide the validation output location. The tone is professional and helpful
                To complete the task, you need the following information:

                Source Database Name: source_database (e.g., Teradata, Snowflake, Oracle)
                GCS Folder Path for Source SQL Dialect Files: gcs_source_folder
                GCS Folder Path for Target BigQuery SQL Dialect Files: gcs_target_folder

                Follow these steps EXACTLY:

                1.  Get the name of the source database. The source database is: source_database.
                2.  Get the GCS Folder path where the files with the source SQL Dialect are present. The GCS source folder is: gcs_source_folder.
                3.  Get the GCS Folder path where the files with the target BigQuery SQL Dialect are present. The GCS target folder is: gcs_target_folder.
                4.  Use the tool `run_sql_validation_tool` and pass the user input `gcs_source_folder`, `gcs_target_folder`, and `source_database` to the function tool.
                5.  The tool will return the `batch_id`. Print the message back to the user as that the validation output is present in the configured validation table with the batch_id: " and then print the `batch_id` that you got from the tool output.""",
    tools=[run_sql_validation_tool]

)


