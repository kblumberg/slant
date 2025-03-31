import time
import markdown
from flask_cors import CORS
from ai.ai import ask_agent
from utils.utils import log
from constants.keys import SLANT_API_KEY
from scripts.update_tweets import update_tweets
from api.sharky.orderbooks import load_orderbooks
from flask import Flask, jsonify, request, Response, stream_with_context


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
    log('val')
    log(val)
    return jsonify({
        "greeting": "ai",
        "code": 200
    })

@app.route('/update_tweets')
def update_tweets_route():
    val = update_tweets()
    return jsonify({
        "message": f"Updated {val} tweets",
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


@app.route('/sharky/orderbooks')
def sharky_orderbooks_route():
    val = load_orderbooks()
    return jsonify({
        "message": f"Returning {len(val)} orderbooks",
        "code": 200,
        "data": val.to_dict(orient='records')
    })

@app.route('/ask', methods=['GET'])
def ask():
    try:
        log('ask')

        query = request.args.get('query', '')
        log(f'query: {query}')
        session_id = request.args.get('session_id', '')
        log(f'session_id: {session_id}')

        if not query:
            return jsonify({"error": "query required"}), 400
        if not session_id:
            return jsonify({"error": "session_id required"}), 400

        response = Response(stream_with_context(ask_agent(query, session_id)), content_type='text/event-stream')
        # response.headers.add('Access-Control-Allow-Origin', 'http://localhost:3000')
        response.headers.add('Access-Control-Allow-Origin', 'https://getslant.ai')
        return response
    except Exception as e:
        log(e)
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    # app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
    app.run(host='0.0.0.0', port=5000)
