import boto3

def handler(event, context):
    # Connect to Athena
    athena = boto3.client("athena")

    # Define the SQL query to run
    query = "SELECT * FROM username"

    # Start an Athena query execution
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": "dataall_nagarro_dhcentrallogs_nczcb7va"
        },
        ResultConfiguration={
            "OutputLocation": "s3://dataall-nagarro-dhcentrallogs-nczcb7va/"
        }
    )
    # Get the query execution ID
    query_execution_id = response["QueryExecutionId"]

    # Wait for the query to finish
    while True:
        # Get the query execution status
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)["QueryExecution"]["Status"]["State"]

        # If the query is finished, break the loop
        if query_status == "SUCCEEDED":
            break

    # Get the query results from S3
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket="dataall-nagarro-dhcentrallogs-nczcb7va", Key=query_execution_id + ".csv")
    file_content = response["Body"].read().decode("utf-8")

    # Convert the query results to a list of dictionaries
    data = []
    lines = file_content.split("\n")
    header = lines[0].split(",")
    for line in lines[1:]:
        if line:
            values = line.split(",")
            data.append(dict(zip(header, values)))

    # Return the data
    return {
        "statusCode": 200,
        "body": data
    }