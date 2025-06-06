import time
import markdown
import os
import requests
import boto3
from flask_cors import CORS
from utils.utils import log
from constants.keys import SLANT_API_KEY
from scripts.update_tweets import update_tweets
from ai.tools.analyst.analyst import ask_analyst
from api.sharky.orderbooks import load_orderbooks
from api.flipside.update_flipside_data import update_flipside_data
from api.news.load_news import load_news
from ai.tools.slant.news_finder import news_finder
from flask import Flask, jsonify, request, Response, stream_with_context
from api.slant.get_upload_url import get_upload_url

app = Flask(__name__)
CORS(app)

@app.route('/')
def home():
    return jsonify({
        "message": "Welcome to my Flask backend!",
        "status": "running"
    })

@app.route('/hello')
def hello():
    return jsonify({
        "greeting": "Hello, World!",
        "code": 200
    })

@app.route('/ai')
def ai():
    val = make_graph()
    # log('val')
    # log(val)
    return jsonify({
        "greeting": "ai",
        "code": 200
    })

@app.route('/api/get-upload-url', methods=['POST'])
def get_upload_url_route():
    data = request.json
    filename = data.get('filename')
    if not filename:
        return jsonify({'error': 'Missing filename'}), 400
    return get_upload_url(filename)


@app.route('/update_tweets')
def update_tweets_route():
    val = update_tweets()
    return jsonify({
        "message": f"Updated {val} tweets",
        "code": 200
    })

@app.route('/load_news')
def load_news_route():
    val = load_news()
    return jsonify({
        "message": f"Loaded {len(val)} news",
        "data": val.to_dict(orient='records'),
        "code": 200
    })

@app.route('/api/update_flipside_data', methods=['POST'])
def update_flipside_data_route():
    try:
        data = request.get_json()
        api_key = data.get("api_key")
        if api_key != SLANT_API_KEY:
            return jsonify({"error": "Invalid API key"}), 401
        val = update_flipside_data()
        return jsonify({
            "message": f"Updated {val} flipside queries",
            "code": 200
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/update_news', methods=['POST'])
def update_news_route():
    try:
        data = request.get_json()
        api_key = data.get("api_key")
        if api_key != SLANT_API_KEY:
            return jsonify({"error": "Invalid API key"}), 401
        val = news_finder()
        return jsonify({
            "message": f"Finished finding news",
            "code": 200
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/sharky/orderbooks')
def sharky_orderbooks_route():
    val = load_orderbooks()
    return jsonify({
        "message": f"Returning {len(val)} orderbooks",
        "code": 200,
        "data": val.to_dict(orient='records')
    })


@app.route('/ask_analyst', methods=['GET'])
def ask_analyst_route():
    try:
        # log('ask_analyst')

        query = request.args.get('query', '')
        # log(f'query: {query}')
        conversation_id = request.args.get('conversation_id', '')
        # log(f'conversation_id: {conversation_id}')
        user_id = request.args.get('user_id', '')
        # log(f'user_id: {user_id}')

        if not query:
            return jsonify({"error": "query required"}), 400
        if not conversation_id:
            return jsonify({"error": "conversation_id required"}), 400
        if not user_id:
            return jsonify({"error": "user_id required"}), 400

        response = Response(stream_with_context(ask_analyst(query, conversation_id, user_id)), content_type='text/event-stream')
        response.headers.add('Access-Control-Allow-Origin', 'http://localhost:3000')
        # response.headers.add('Access-Control-Allow-Origin', 'https://getslant.ai')
        return response
    except Exception as e:
        # log(e)
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    # app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
    app.run(host='0.0.0.0', port=5000)
