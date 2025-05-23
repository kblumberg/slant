import os
from dotenv import load_dotenv

load_dotenv()

LANGCHAIN_TRACING_V2 = False

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SLANT_API_KEY = os.getenv("SLANT_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
POSTGRES_ENGINE = os.getenv("POSTGRES_ENGINE")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
FLIPSIDE_API_KEY = os.getenv("FLIPSIDE_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
LANGCHAIN_API_KEY = os.getenv("LANGCHAIN_API_KEY")

TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
TWITTER_BEARER_TOKEN_NDS = os.getenv("TWITTER_BEARER_TOKEN_NDS")
TWITTER_BEARER_TOKEN_TRAILS = os.getenv("TWITTER_BEARER_TOKEN_TRAILS")
TWITTER_BEARER_TOKEN_ASH = os.getenv("TWITTER_BEARER_TOKEN_ASH")
ACTIVE_TWITTER_TOKENS = [TWITTER_BEARER_TOKEN_NDS]