import json
import base64
import logging
from google.cloud import storage, pubsub_v1

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def mapper_trigger(event, context):
    try:
        storage_client = storage.Client()
        event_data = json.loads(base64.b64decode(event['data']).decode('utf-8'))

        # Check if 'file_name' and 'chunk_id' are present in the message data
        if 'file_name' not in event_data or 'chunk_id' not in event_data:
            logger.warning('Invalid message format: Missing file_name or chunk_id')
            return

        file_name = event_data['file_name']
        chunk_id = event_data['chunk_id']
        bucket_name = 'amit-bucket-b561'
        document_name = f'{file_name}_chunk_{chunk_id}'

        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(f'{file_name}_chunk_{chunk_id}')
        chunk_contents = blob.download_as_text()

        logger.info(f'Processing chunk {chunk_id} of file {file_name}')

        mapped_data = map_function(document_name, chunk_contents)

        intermediate_storage_client = storage.Client()
        intermediate_bucket = intermediate_storage_client.bucket('intermediate-results-bucket')
        intermediate_blob = intermediate_bucket.blob(f'intermediate_{file_name}_{chunk_id}.json')
        intermediate_blob.upload_from_string(json.dumps(mapped_data))

        publish_mapper_completion(file_name)

        logger.info(f'Completed processing chunk {chunk_id} of file {file_name}')
    except Exception as e:
        logger.error(f"Error processing chunk {event_data.get('chunk_id')} of file {event_data.get('file_name')}: {e}")

# def map_function(document_contents):
#     words = document_contents.split()
#     return [(word.lower(), 1) for word in words]

def map_function(document_name, document_contents):
    words = document_contents.split()
    return [(word.lower(), document_name) for word in words]


def publish_mapper_completion(file_name):
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_name = 'projects/amit-singh-fall2023-403819/topics/mapper-completion'  
        message = json.dumps({'file_name': file_name, 'status': 'completed'})
        publisher.publish(topic_name, message.encode('utf-8'))
    except Exception as e:
        logger.error(f"Error publishing completion message for file {file_name}: {e}")

