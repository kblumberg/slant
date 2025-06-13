
from flask import jsonify
from utils.utils import log
from utils.db import pg_load_data

def load_conversations(user_id: str):
    log(f'loading conversations from {user_id}')

    query = f"""
    SELECT * FROM conversations WHERE user_id = '{user_id}' ORDER BY updated_at DESC LIMIT 10
    """
    df = pg_load_data(query)

    try:
        return jsonify({
            'conversations': df.to_dict(orient='records')
            , 'code': 200
        })

    except Exception as e:
        print(f"Error generating presigned URL: {e}")
        return jsonify({'error': 'Could not generate URL'}), 500

