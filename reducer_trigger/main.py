import json
import logging
from google.cloud import storage
from collections import defaultdict


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def reducer_trigger(event, context):
    storage_client = storage.Client()
    intermediate_bucket = storage_client.get_bucket('intermediate-results-bucket')
    blobs = intermediate_bucket.list_blobs()
    all_mapped_data = []

    try:
        for blob in blobs:
            content = json.loads(blob.download_as_text())
            all_mapped_data.extend(content)
            logger.info(f'Processed blob {blob.name}')

        new_reduced_data = reduce_function(all_mapped_data)
        logger.info('New reduced data computed successfully')

        # Read existing data from final_reduced_data.json
        final_storage_client = storage.Client()
        final_bucket = final_storage_client.bucket('final-results-bucket')
        final_blob = final_bucket.blob('final_reduced_data.json')
        
        existing_data = {}
        if final_blob.exists():
            existing_data = json.loads(final_blob.download_as_text())

        # Merge new reduced data with existing data
        merged_data = merge_data(existing_data, new_reduced_data)

        # Upload merged data
        final_blob.upload_from_string(json.dumps(merged_data))
        logger.info('Final reduced data uploaded successfully')
    except Exception as e:
        logger.error(f"Error during reduce phase: {e}")
        # Additional error handling can be implemented here

# def reduce_function(mapped_data):
#     inverted_index = defaultdict(list)
#     for word, _ in mapped_data:
#         inverted_index[word].append(1)

#     search_results = {word: sum(counts) for word, counts in inverted_index.items()}
#     return search_results

def reduce_function(mapped_data):
    inverted_index = defaultdict(lambda: defaultdict(int))
    for word, doc_name in mapped_data:
        inverted_index[word][doc_name] += 1
    return inverted_index

def merge_data(existing_data, new_data):
    for word, doc_counts in new_data.items():
        if word not in existing_data:
            existing_data[word] = doc_counts
            continue

        if not isinstance(existing_data[word], dict):
            logger.error(f"Expected existing_data[{word}] to be a dict, but got {type(existing_data[word])}")
            continue

        for doc, count in doc_counts.items():
            if doc not in existing_data[word]:
                existing_data[word][doc] = count
            else:
                if not isinstance(existing_data[word][doc], int):
                    logger.error(f"Expected existing_data[{word}][{doc}] to be an int, but got {type(existing_data[word][doc])}")
                    continue
                existing_data[word][doc] += count
    return existing_data

