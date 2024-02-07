import json
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from google.cloud import storage
import math
from collections import Counter, defaultdict
import logging

app = Flask(__name__)
CORS(app)

# CORS(app, resources={r"/*": {"origins": "https://storage.googleapis.com"}})


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

@app.route('/front_end', methods=['POST', 'OPTIONS'])
def search_documents(request):
    logger.info('INSIDE SEARCH_DOCUMENTS')
    if request.method == 'OPTIONS':
        logger.info('YUPP OPTIONS')
        return after_request(make_response())
    try:
        logger.info('TRYING')
        request_json = request.get_json(silent=True)
        search_query = request_json.get('query', '')
        logger.info(f'search_query type: {type(search_query)}')
        search_query = search_query.strip().lower()

        if not search_query:
            return jsonify({"error": "No search query provided"}), 400

        processed_data = fetch_processed_data()
        
        logger.info('SO FAR SO GOOD')

        if not processed_data:
            logger.info('NOT PROCESSED DATA')
            return jsonify({"error": "Failed to fetch processed data"}), 500

        results = search_and_rank(processed_data, search_query)
        logger.info('ALMOST DONE, results type:', type(results))
        return jsonify(results), 200, {'Access-Control-Allow-Origin': '*'}

    except Exception as e:
        logger.error(f"Error in 1: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

    except Exception as e:
        logger.error(f"Error in 2: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

def fetch_processed_data():
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket('final-results-bucket')
        blob = bucket.blob('final_reduced_data.json')
        data = blob.download_as_text()
        documents = json.loads(data)
        return documents
    except Exception as e:
        logger.error(f"Error fetching processed data: {str(e)}")
        return None

def calculate_tf_idf(documents):
    idf_scores = calculate_idf(documents)
    tf_idf_scores = defaultdict(dict)

    for doc_id, document in enumerate(documents):
        tf_scores = calculate_tf(document)
        for word, tf_score in tf_scores.items():
            tf_idf_scores[doc_id][word] = tf_score * idf_scores[word]

    return tf_idf_scores

def calculate_tf(document):
    tf_scores = {}
    total_words = len(document)
    word_counts = Counter(document)

    for word, count in word_counts.items():
        tf_scores[word] = count / total_words
    return tf_scores

def calculate_idf(documents):
    total_documents = len(documents)
    word_document_counts = Counter(word for document in documents for word in set(document))

    idf_scores = {word: math.log(total_documents / doc_count) for word, doc_count in word_document_counts.items()}
    return idf_scores


def search_and_rank(inverted_index, search_query):
    if search_query in inverted_index:
        doc_freqs = inverted_index[search_query]

        # Aggregate counts for each file
        file_counts = defaultdict(int)
        for doc_chunk, count in doc_freqs.items():
            file_name = doc_chunk.split('_chunk_')[0]  # Extracting file name from chunk name
            file_counts[file_name] += count

        # Sort files by aggregated count and return top 5
        sorted_files = sorted(file_counts.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_files) >= 5:
            return [file[0] for file in sorted_files[:5]]
        return [file[0] for file in sorted_files]
    return []




if __name__ == '__main__':
    app.run(debug=True)
