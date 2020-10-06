#-----------------------------------------------------------------------------
# © 2020 BlueVenn Ltd
#-----------------------------------------------------------------------------
#	Component	:	BlueVenn.Indigo.AWSListener
#-----------------------------------------------------------------------------
#	Version History
#	---------------
#	Date		Author		Xref.		Notes
#	----		------		-----		-----
#   29/09/2020  J.Boyce     -           First Release
#-----------------------------------------------------------------------------
import os
import os.path
import sys

# Add locally installed boto3
LAMBDA_TASK_ROOT = os.environ["LAMBDA_TASK_ROOT"]
sys.path.insert(0, LAMBDA_TASK_ROOT+"/boto3")
import botocore
import boto3
import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get the ARNs encrypted in environment variables
cluster_arn = os.environ['AURORA_LISTENER_CLUSTER_ARN']
secret_arn = os.environ['AURORA_LISTENER_SECRET_ARN']

# This function is called from API gateway directly and hence will pull its parameters from
# the queryStringParameters as defined by the API gateway proxy integration.
# Paramters on the call are:
# orgId: A named entity used to segment the listener by client/usage etc
# firstEvent: The earliest event to fetch
# maxEventsPerCall: The maximum number of events to fetch in a single call
def lambda_handler(event, context):
#    logger.info(event)    
    try:
        rds_data = boto3.client('rds-data')
        sqs = boto3.client('sqs')
        
		# Extract params from query string
        orgId = event["queryStringParameters"]['orgId']
        firstEvent = int( event["queryStringParameters"]['firstEvent'] )
        maxEventsPerCall = int( event["queryStringParameters"]['maxEventsPerCall'] )
        
        tableId = "RxEvents" + orgId

        response = rds_data.execute_statement(
            resourceArn = cluster_arn, 
            secretArn = secret_arn, 
            database = 'ListenerDB', 
            sql = f""" select eventid, eventdata from {tableId} where eventid > :eventidparam order by eventid asc limit :maxpercallparam """,
            parameters = [{'name':'tableidparam', 'value':{'stringValue': tableId}},
                {'name':'eventidparam', 'value':{'longValue': firstEvent}},{'name':'maxpercallparam', 'value':{'longValue': maxEventsPerCall}}])
        
        # Build response data          
        data = []
        
        if 'records' in response:
            for currRow in response['records']:
                newRow = {}
                for rowData in currRow:
                    for dataType, dataValue in rowData.items():
                        if dataType == "longValue":
                            newRow['eventId'] = dataValue;
                        elif dataType == "stringValue":
                            newRow['eventData'] = json.loads( dataValue );
                data.append(newRow)                
                
        return {
            "statusCode": 200,
            "body": json.dumps( data ),
            "headers":{ 'Access-Control-Allow-Origin' : '*' } }
    
    except Exception as e:
        
         raise e
