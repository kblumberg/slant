import news_finder

def generate_news(start_timestamp: int):
    prompt = f"""
    You are an expert at ingesting a list of tweets and news articles about the solana blockchain ecosystem and generating a list of news articles that are relevant to the ecosystem.
    
    Here 
    """
    news_df = news_finder.news_finder(start_timestamp)
    print(news_df)

if __name__ == "__main__":
    generate_news()
