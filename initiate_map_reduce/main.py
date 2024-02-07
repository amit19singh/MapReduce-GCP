import base64
import json
import logging
from google.cloud import storage, pubsub_v1, firestore
from flask import escape
from flask import Flask, request, jsonify
# from flask_cors import CORS  


# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

app = Flask(__name__)

# CORS(app, resources={r"/initiate_map_reduce": {"origins": "http://localhost:9889"}})

# @app.route('/initiate_map_reduce', methods=['OPTIONS'])
# def cors_preflight():
#     return ('', 204)


def initiate_map_reduce(request):
    try:
        if request.method != 'POST':
            raise ValueError('Invalid request method')

        # Ensure 'multipart/form-data' content type
        if 'multipart/form-data' not in request.content_type:
            raise ValueError('Invalid content type. Must be multipart/form-data.')


        # Form data
        num_mappers = int(request.form.get('numMappers', ''))
        num_reducers = int(request.form.get('numReducers', ''))
        search_word = request.form.get('searchWord', '')

        # File data
        uploaded_file = request.files.get('fileUpload')
        if not uploaded_file:
            raise ValueError('No file uploaded')

        db = firestore.Client()
        file_name = uploaded_file.filename

        # Create a Firestore document with the file_name as the document ID
        doc_ref = db.collection('mapper_completion').document(file_name)

        # Set initial values for total_mappers and completed_count
        doc_ref.set({
            'total_mappers': num_mappers,
            'completed_count': 0
        })

        # Validations
        if num_mappers > 25 or num_reducers > 25:
            raise ValueError('Number of mappers and reducers cannot exceed 25')
        if num_reducers >= num_mappers:
            raise ValueError('Number of reducers must be less than the number of mappers')

        file_content = uploaded_file.read().decode('utf-8')
        chunks = split_text_into_chunks(file_content, num_mappers)

        # Cloud Storage
        storage_client = storage.Client()
        bucket_name = 'amit-bucket-b561'
        bucket = storage_client.bucket(bucket_name)

        if bucket.blob(file_name).exists():
            raise ValueError('File with the same name already exists in the bucket')


        for i, chunk in enumerate(chunks):
            # Define a unique object name for each chunk
            object_name = f'{uploaded_file.filename}_chunk_{i}'
            
            # Upload the chunk to Cloud Storage
            # bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            blob.upload_from_string(chunk)

            # Pub/Sub
            publisher = pubsub_v1.PublisherClient()
            topic_name = 'projects/amit-singh-fall2023-403819/topics/ECC_Assignment4'

            message_data = {
                'file_name': uploaded_file.filename,
                'chunk_id': i,
                'search_word': search_word,
                'cloud_storage_object': f'gs://{bucket_name}/{object_name}'
            }
            message = json.dumps(message_data)
            publisher.publish(topic_name, message.encode('utf-8'))

        logger.info(f'Map-reduce process initiated for {uploaded_file.filename}.')
        # return {'message': f'Map-reduce process initiated for {uploaded_file.filename}.'}, 200
        return {'message': f'Map-reduce process initiated for {uploaded_file.filename}.'}, 200, {'Access-Control-Allow-Origin': '*'}

    except ValueError as e:
        logger.error(f'Error: {str(e)}')
        return {'error': str(e)}, 400
    except Exception as e:
        logger.error(f'Unexpected error: {str(e)}')
        return {'error': 'Internal server error'}, 500

def split_text_into_chunks(text, num_chunks):
    chunk_size = len(text) // num_chunks
    chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]
    return chunks


