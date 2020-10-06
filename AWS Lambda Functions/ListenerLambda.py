#-----------------------------------------------------------------------------
# © 2020 BlueVenn Ltd
#-----------------------------------------------------------------------------
#	Component	:	BlueVenn.Indigo.AWSListener
#-----------------------------------------------------------------------------
#	Version History
#	---------------
#	Date		Author		Xref.		Notes
#	----		------		-----		-----
#   30/09/2020  J.Boyce     -           First Release
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

# This function takes a JSON data block and without understanding its content stores it
# in an Aurora Serverless DB.  Note: the call to this comes from an SQS queue and will contain
# not only the data to store but also an 'OrgId' within the messageAttributes.  This OrgId can
# be used to segment the listener for multiple clients/usage scenarios etc.
def lambda_handler(event, context):
#    logger.info(event)    
    try:
        rds_data = boto3.client('rds-data')
        sqs = boto3.client('sqs')

        # Check a message
        if 'Records' not in event:
            return { 'statusCode':400, 'body':'Bad Request' }

        for record in event['Records']:
            if 'messageId' not in record:
                continue
            if 'body' not in record:
              continue
            if 'messageAttributes' not in record:
                continue
            if 'OrgId' not in record['messageAttributes']:
                continue

            # Extact useful details
            msgId = record['messageId']
            body = str( record['body'] )
            msgAttribs = record['messageAttributes']
            orgId =  msgAttribs['OrgId'] 
            tableId = "RxEvents" + orgId['stringValue']

            response = rds_data.execute_statement(
                resourceArn = cluster_arn, 
                secretArn = secret_arn, 
                database = 'ListenerDB', 
                sql = f""" insert into `{tableId}` ( MessageId, EventData ) Values( :msgidparam, :dataparam ) """,
                                parameters = [{'name':'msgidparam', 'value':{'stringValue': msgId}},{'name':'dataparam', 'value':{'stringValue': body}}])
                                
            # Check nothing in the dlq
            queue_url = sqs.get_queue_url( QueueName='ListenerDeadLetterQueue' )['QueueUrl']
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=['SentTimestamp'],
                MaxNumberOfMessages=10,
                MessageAttributeNames=['All'],
                VisibilityTimeout=0,
                WaitTimeSeconds=0 );
                
            if 'Messages' in response:
                for message in response['Messages']:
                    logger.info(message)
                    
                    receipt_handle = message['ReceiptHandle']
                    
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle );
            
        return { 'statusCode':200, 'body':'Success' }
    except Exception as e:
        
         raise e
