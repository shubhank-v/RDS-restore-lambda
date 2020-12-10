import boto3
import botocore
from botocore import exceptions
import datetime
import re
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


REGION = '{}'
DB_INSTANCE_CLASS = '{}'
DB_SUBNET = '{}'
PRODUCTION_INSTANCE = '{}'
PRE_PRODUCTION_INSTANCE = 'preproduction-{}'.format(PRODUCTION_INSTANCE)



# Helper function to sort snapshot by timestamp
def byTimestamp(snap):
    if 'SnapshotCreateTime' in snap:
        return datetime.datetime.isoformat(snap['SnapshotCreateTime'])
    else:
        return datetime.datetime.isoformat(datetime.datetime.now())


def lambda_handler(event, context):  
    rds_client = boto3.client('rds')
    try:
        # Retrieve the latest snapshot
        source_snaps = rds_client.describe_db_snapshots(DBInstanceIdentifier=PRODUCTION_INSTANCE)['DBSnapshots']
        source_snap = sorted(source_snaps, key=byTimestamp, reverse=True)[0]['DBSnapshotIdentifier']
        snap_id = (re.sub('-\d\d-\d\d-\d\d\d\d ?', '', source_snap))
        logger.info('Will restore {} to {}'.format(source_snap, snap_id))
        
        
        # Delete the pre-prod instance
        logger.info('Deleting pre-prod snapshot')
        rds_client.delete_db_instance(DBInstanceIdentifier=PRE_PRODUCTION_INSTANCE,
                                SkipFinalSnapshot=True,
                                DeleteAutomatedBackups=True)
        try:
            waiter = rds_client.get_waiter('db_instance_deleted')
            logger.info('Waiting for instance termination...')
            waiter.wait(DBInstanceIdentifier=PRE_PRODUCTION_INSTANCE)
        except botocore.exceptions.WaiterError as e:
            logger.info(e)
        
        
        
        # Restore pre-prod db instance from the prod snapshot
        logger.info('creating instance from prod')
        response = rds_client.restore_db_instance_from_db_snapshot(DBInstanceIdentifier=PRE_PRODUCTION_INSTANCE,
                                                      DBSnapshotIdentifier=source_snap,
                                                      DBInstanceClass=DB_INSTANCE_CLASS,
                                                      DBSubnetGroupName=DB_SUBNET,
                                                      MultiAZ=False,
                                                      PubliclyAccessible=True,
                                                      DeletionProtection=False)
                                                      
                                                      
        sns_client = boto3.client('sns')
        message = '{} Successfully restored from {}'.format(PRE_PRODUCTION_INSTANCE,snap_id)
        response = sns_client.publish(
        TopicArn='{}',
        Message=message,
        Subject='Snapshot Restored into Pre-Production Instance',
        )

        return message


    except exceptions.ClientError as e:
        raise Exception("Could not restore: %s" % e)

