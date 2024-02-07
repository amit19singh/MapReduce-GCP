import json
import base64
import logging
import time
from google.cloud import pubsub_v1, firestore

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def track_mapper_completion(event, context):
    logger.info('STARTING')
    try:
        logger.info('TRYING')
        event_data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        file_name = event_data['file_name']

        db = firestore.Client()
        doc_ref = db.collection('mapper_completion').document(file_name)

        while True:
            transaction = db.transaction()
            completed_count, total_mappers = update_in_transaction(transaction, doc_ref)
            
            if completed_count is not None and total_mappers is not None:
                if completed_count >= total_mappers:
                    logger.info('ALL MAPPERS COMPLETED, TRIGGERING REDUCER')
                    trigger_reducer()
                    break
                else:
                    logger.info('NOT ALL MAPPERS COMPLETED YET, WAITING...')
                    time.sleep(5)  # Check every 5 seconds. Adjust as needed.
            else:
                logger.info('DOCUMENT DOES NOT EXIST OR ERROR IN TRANSACTION')
                break

    except Exception as e:
        logger.error(f'Error in track_mapper_completion: {e}')

@firestore.transactional
def update_in_transaction(transaction, doc_ref):
    logger.info('INSIDE update_in_transaction')
    snapshot = doc_ref.get(transaction=transaction)
    if snapshot.exists:
        logger.info('INSIDE FIRST IF, LINE 25')
        transaction.update(doc_ref, {
            'completed_count': firestore.Increment(1)
        })
        completed_count = snapshot.get('completed_count')
        total_mappers = snapshot.get('total_mappers')
        logger.info(f'completed_count: {completed_count}, {type(completed_count)}')
        logger.info(f'total_mappers: {total_mappers}, {type(total_mappers)}')

        return completed_count, total_mappers
    else:
        # Create the document with initial values
        total_mappers = 0 # Replace with logic to determine total_mappers
        doc_ref.set({
            'total_mappers': total_mappers,
            'completed_count': 1
        }, merge=True)  # Use merge=True to create or update the document
        logger.info('CREATED NEW DOCUMENT')
        return 1, total_mappers

def trigger_reducer():
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_name = 'projects/amit-singh-fall2023-403819/topics/reducer-topic'
        message = json.dumps({'trigger': 'start-reducer'})
        publisher.publish(topic_name, message.encode('utf-8'))
        logger.info('TRIGGERED THE REDUCER')
    except Exception as e:
        logger.error(f'Error triggering reducer: {e}')
