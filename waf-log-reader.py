#!/usr/bin/python
import os,sys
from dotenv import load_dotenv
import requests
import mysql.connector
import time
from datetime import datetime, timedelta

####################################
# see waf-log-reader.md and README
# for more information on how to use
####################################

load_dotenv()

def add_one_hour_to_timestamp(timestamp_str):
    # Parse the input timestamp string to a datetime object
    original_timestamp = datetime.fromisoformat(timestamp_str)

    # Add one hour to the original timestamp
    updated_timestamp = original_timestamp + timedelta(hours=1)

    # Convert both timestamps to strings
    original_timestamp_str = original_timestamp.isoformat()[:-6] + 'Z'
    updated_timestamp_str = updated_timestamp.isoformat()[:-6] + 'Z'

    return original_timestamp_str, updated_timestamp_str

CDN_PROVIDER_API_ENDPOINT = "https://manager.azion.com/events/graphql"
CDN_PROVIDER_TOKEN_ENV_VAR_NAME = "azion_personal_token"

# check config environment for API KEY
if CDN_PROVIDER_TOKEN_ENV_VAR_NAME not in os.environ:
    sys.exit(f'Environment variable "{CDN_PROVIDER_TOKEN_ENV_VAR_NAME}" not found!')
else:
    cdn_provider_auth_token = os.environ[CDN_PROVIDER_TOKEN_ENV_VAR_NAME]

# Set the request headers
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Token {cdn_provider_auth_token}"
}

# check config environment for DB KEYS
DB_HOST_VAR_NAME = "aws_rds_host"
DB_USERNAME_VAR_NAME = "aws_rds_username"
DB_PASSWORD_VAR_NAME = "aws_rds_password"
DB_DATABASE_VAR_NAME = "aws_rds_database"

if DB_HOST_VAR_NAME not in os.environ:
    sys.exit(f'Environment variable "{DB_HOST_VAR_NAME}" not found!')
if DB_USERNAME_VAR_NAME not in os.environ:
    sys.exit(f'Environment variable "{DB_USERNAME_VAR_NAME}" not found!')
if DB_PASSWORD_VAR_NAME not in os.environ:
    sys.exit(f'Environment variable "{DB_PASSWORD_VAR_NAME}" not found!')
if DB_DATABASE_VAR_NAME not in os.environ:
    sys.exit(f'Environment variable "{DB_DATABASE_VAR_NAME}" not found!')

# validate (1) hosts_list and first run datetime, (2) GraphQL endpoint and credentials, and (3) MySQL credentials

## (1) hosts list and first run datetime
HOSTS_LIST_VAR_NAME = "hosts_list"
if HOSTS_LIST_VAR_NAME not in os.environ or len((os.environ[HOSTS_LIST_VAR_NAME].strip()))==0:
    # empty hosts list, abort
    sys.exit(f'Environment variable "{DB_DATABASE_VAR_NAME}" not found or empty!')

hosts_list_filter = os.environ[HOSTS_LIST_VAR_NAME].strip()

FIRST_RUN_DT_VAR_NAME = "first_run_datetime"
if os.environ[FIRST_RUN_DT_VAR_NAME]:
    first_run_datetime = os.environ[FIRST_RUN_DT_VAR_NAME]
else:
    # make this dynamic and based on the current date and time minus X days (param)
    first_run_datetime = "2024-04-08T00:00:00Z"

## (2) GraphQL enpoint and credentials
validate_graphql_query= {
    "query": f'query HttpQuery {{httpEvents(limit: {1}, filter: {{tsRange: {{begin:"2024-04-01T00:00:00Z", end:"2024-04-01T01:00:00Z"}}}}, orderBy: [ts_ASC]) {{ts remoteAddress httpUserAgent host requestUri stacktrace geolocCountryName geolocRegionName status sslCipher httpReferer upstreamResponseTime upstreamResponseTimeStr upstreamBytesReceivedStr requestTime wafBlock wafScore}}}}',
    "variables": None,
    "operationName": "HttpQuery"
  }

try:
    validate_credentials_response = requests.post(CDN_PROVIDER_API_ENDPOINT, headers=headers, json=validate_graphql_query)
    validate_credentials_response.raise_for_status()
except requests.exceptions.HTTPError as err:
    exit(f"Something went wrong with GraphQL credentials.\nEndPoint={CDN_PROVIDER_API_ENDPOINT}\n")

print(f"Successfully Connected to GraphQL EndPoint at {CDN_PROVIDER_API_ENDPOINT}")

## (3) MySQL credentials
try:
    mydb = mysql.connector.connect(
    host=os.environ[DB_HOST_VAR_NAME],
    user=os.environ[DB_USERNAME_VAR_NAME],
    password=os.environ[DB_PASSWORD_VAR_NAME],
    database=os.environ[DB_DATABASE_VAR_NAME]
    )
except mysql.connector.Error as err:
  exit(f"Something went wrong with MySQL Connection: {err}")

print(f"Successfully Connected to MySQL Database at {os.environ[DB_HOST_VAR_NAME]}")

# Find last timestamp for hosts lists in table

last_TS_query = f"""SELECT MAX(requestTS) FROM httpRequests WHERE requestHost in ('{hosts_list_filter}');"""
mycursor = mydb.cursor()
mycursor.execute(last_TS_query)
last_TS_recordset = mycursor.fetchone()

if last_TS_recordset[0] is not None:
    last_time_stamp = last_TS_recordset[0]
    print(f"Last TimeStamp found for {hosts_list_filter} was {last_time_stamp}")
else:
    last_time_stamp = first_run_datetime
    print(f"No TimeStamp found for {hosts_list_filter} using first run date instead {first_run_datetime}!")

if mycursor: mycursor.close()

original_ts, updated_ts = add_one_hour_to_timestamp(last_time_stamp)

target_time_stamp = updated_ts
print (f"Target TimeStamp is {target_time_stamp}")

GRAPHQL_BATCH_SIZE = 10000
running_datetime = last_time_stamp

while datetime.fromisoformat(running_datetime.replace("Z", "+00:00")) != datetime.fromisoformat(target_time_stamp.replace("Z", "+00:00")):
  # GraphQL Query for HTTP Requests
  json_query= {
    "query": f'query HttpQuery {{httpEvents(limit: {GRAPHQL_BATCH_SIZE}, filter: {{tsRange: {{begin:"{running_datetime}", end:"{target_time_stamp}"}}  host: "{hosts_list_filter}"}}, orderBy: [ts_ASC]) {{ts remoteAddress httpUserAgent host requestUri stacktrace geolocCountryName geolocRegionName status sslCipher httpReferer upstreamResponseTime upstreamResponseTimeStr upstreamBytesReceivedStr requestTime wafBlock wafScore}}}}',
    "variables": None,
    "operationName": "HttpQuery"
  }

  
  # Make the HTTP request
  gql_start = time.time()
  print(f"Sending Query to GraphQL Endpoint filtering for host {hosts_list_filter}...")
  response = requests.post(CDN_PROVIDER_API_ENDPOINT, headers=headers, json=json_query)
  gql_finish = time.time()
  print(f"GraphQL Time taken: \t{(gql_finish-gql_start)*10**3:.03f}ms")
  
  if response.status_code == 200:
      data = response.json()
      if 'data' in data and 'httpEvents' in data['data']:
          access_logs = data['data']['httpEvents']
          number_of_records = len(access_logs)
          first_ts = ""
          print("-- Running SQL Statements against DB")
          mycursor = mydb.cursor()
          sql_start = time.time()
          for log in access_logs:
                if first_ts == "": 
                    first_ts= log['ts']
                last_ts = log['ts']
                l = log
                sqlStatement = f"""INSERT INTO httpRequests (requestTS, requestHost, requestUri, remoteAddress, httpUserAgent, httpStatus, wafScore, wafBlock, requestDuration, geolocCountryName, geolocRegionName) VALUES ("{l['ts']}","{l['host']}","{l['requestUri']}","{l['remoteAddress']}","{l['httpUserAgent']}",{l['status']},"{l['wafScore']}","{l['wafBlock']}",{l['requestTime']},"{l['geolocCountryName']}","{l['geolocRegionName']}");"""
                
                try:
                    mycursor.execute(sqlStatement)
                except mysql.connector.Error as err:
                    print (f"SQL Error: {err}")
          sql_finish = time.time()
          last_ts_obj = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
          first_ts_obj = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))

          print(f"SQL Time taken: \t{(sql_finish-sql_start)*10**3:.03f}ms")
          print(f"DataSet Time Span:\t{last_ts_obj-first_ts_obj}")
          print(f"Batch Time taken: \t{(sql_finish-gql_start)*10**3:.03f}ms")

          mydb.commit()

          running_datetime = last_ts

      else:
          print("No access logs found for the specified time range.")
  else:
      print("Failed to retrieve data. Status code:", response.status_code)
      print("Response:", response.text)

# free resources
if mycursor: mycursor.close()
if mydb: mydb.close()
