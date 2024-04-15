# Python Script for Data Ingestion from CDN Provider and Storing in MySQL

  

This Python script is designed to fetch data from a CDN provider's GraphQL endpoint, filter it based on specified criteria, and store it in a MySQL database. Below is a description of the script's functionality and instructions on how to configure it for usage. Inspired on https://awstip.com/how-to-use-python-to-deal-with-amazon-aurora-and-amazon-rds-with-crud-examples-64bb6ccb7d48

  

## Functionality

  

-  **Data Retrieval**: The script sends a GraphQL query to the CDN provider's endpoint to fetch HTTP events data.

-  **Data Filtering**: It filters the retrieved data based on specified criteria such as timestamp range and host.

-  **Data Storage**: The filtered data is then inserted into a MySQL database table named `httpRequests`.

-  **Continuous Execution**: The script continuously fetches data in batches until it reaches the specified target timestamp.

  

## Configuration

  

1.  **Python Environment**:

- Ensure Python 3.11 or higher is installed.

```
$ sudo alternatives --install /usr/bin/python python /usr/bin/python3.9 2
$ sudo alternatives --install /usr/bin/python python /usr/bin/python3.11 1
$ sudo alternatives --config python
$ python3.11 -m ensurepip
```

- Install necessary dependencies using pip:

```
pip install requests python-dotenv mysql-connector-python
```

  

2.  **Environment Variables**:

- Create a `.env` file in the same directory as the script with the following format:

```
aws_rds_host=database-1.cluster-c5yu88e00e5v.sa-east-1.rds.amazonaws.com
aws_rds_username=username
aws_rds_password=xxxxxxxxxxxxxxx
aws_rds_database=database_name
azion_personal_token=aziond6b39f9bc09e99802fXXXXXXXXXXXXXXXXX
hosts_list=vtexfashion
first_run_datetime=2024-04-01T00:00:00Z
```

-  `aws_rds_host`: MySQL database host.

-  `aws_rds_username`: MySQL username.

-  `aws_rds_password`: MySQL password.

-  `aws_rds_database`: MySQL database name.

-  `azion_personal_token`: Personal token for accessing the CDN provider's GraphQL API.

-  `hosts_list`: List of hosts to filter data for.
-  `first_run_datetime`: Date and time from which to start fetching data (ISO 8601 format).

  

3.  **MySQL Database**:

- Ensure MySQL server is running and accessible.

- Create a database named `database_name` (replace with actual database name).

- Create a table named `httpRequests` with appropriate columns to store HTTP request data.

4.  **Running the Script**:

- Execute the script using Python:

```
python waf-log-reader.py
```

- The script will continuously fetch and store data until it reaches the target timestamp.

  

## Additional Notes

  

- The script assumes the existence of a MySQL database and table for storing data. Ensure proper configurations and permissions are set up.

- Make sure to adjust the GraphQL query and database schema according to your specific requirements.

- Monitor script execution and database performance, especially for large datasets, to ensure smooth operation.

- Adjust the time range and other parameters as needed for specific use cases.

- Create a .env file in the same directory as the script

  

## Sample .env file structure:

```
azion_personal_token=xxxxxxxxxxxxxxxx
aws_rds_host=database-2.hostid.region.rds.amazonaws.com
aws_rds_username=username
aws_rds_password=userpassword
aws_rds_database=databasename
hosts_list=vtexfashion.vtex.app
first_run_datetime=2024-04-08T00:00:00Z
```
  

## TO-DOS:

- Check if the GraphQL request was successful

- EDGE CASES ARE:

-  **(1) Too huge response set**

- response.status_code=400

- response={"detail": "An error occured while performing the requested operation.: Limit for result exceeded, max bytes: 2.79 GiB, current bytes: 2.80 GiB processing query."}

-  **(2) Empty response set**

- if no records are found!!! for instance more than 1 week old request...