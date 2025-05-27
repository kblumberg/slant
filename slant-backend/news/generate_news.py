import pandas as pd
from datetime import datetime
from langchain_openai import ChatOpenAI
from utils.db import pg_load_data
from ai.tools.twitter.rag_search_tweets import rag_search_tweets_fn
import json
import re
import requests
from bs4 import BeautifulSoup
from tavily import TavilyClient
from constants.keys import TAVILY_API_KEY
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from utils.db import pg_upload_data

def generate_rag_search_query(text: str) -> str:
    prompt = f"""
    You are an expert AI assistant trained to generate high-quality search queries for a RAG (Retrieval-Augmented Generation) system powered by Pinecone. Your job is to extract the most relevant and informative keywords or short phrases from a given tweet or group of tweets, and format them into a single search query that will retrieve other related tweets.

    - Your query should be concise (ideally under 10 words), use natural language or keyword-style phrasing, and focus on the **main topic**, **named entities**, **hashtags**, and any **unique phrasing** or **terminology** that would help retrieve related content.
    - Do not include links, emojis, or author names.
    - Avoid generic filler (e.g., "check this out", "wow").
    - All the tweets are about the solana blockchain ecosystem, so avoid general terms (e.g., "blockchain", "crypto", "solana").

    Here is the tweet text:
    {text}

    Return only the search query.
    """
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    response = llm.invoke(prompt)
    return response.content

def generate_news(clean_tweets: pd.DataFrame, n_days: int):
    tavily_client = TavilyClient(api_key=TAVILY_API_KEY)
    # tweets = [f'Tweet {i}: {tweet[:1000]}' for i, tweet in enumerate(tweets)]
    # prompt = f"""
    # You are an expert at ingesting a tweets about the solana blockchain ecosystem and identifying which ones are important news articles such as new product launches, feature announcements, updates to the solana blockchain ecosystem, etc.
    
    # Here is a list of tweets:
    # {tweets}

    # Your job is to return a valid JSON list of integers representing the indices of the tweets that are important news articles.

    # Example output:
    # [0, 3, 6, 10, 16, 19, 20]
    # """
    # # llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    # llm = ChatOpenAI(model="gpt-4.1", temperature=0)
    # response = llm.invoke(prompt)
    # print(response.content)

    tweets = ''
    for row in clean_tweets.itertuples():
        tweets += f'# Conversation ID: {row.conversation_id}\n## Author: {row.username}\n## Text: {row.text}\n\n\n'

    prompt = f"""
    You are an expert at ingesting tweets about the solana blockchain ecosystem and generating a list of headlines for the major news stories as well as the twitter urls for the tweets that support the headlines.

    Prioritize news articles such as new product launches, announcements, updates to the solana blockchain ecosystem, etc.

    Avoid posts without a tangiable news story.
    
    Here is a list of tweets:
    {tweets}

    Your job is to return a valid JSON list of objects with the following fields:
    - headline: a string representing the headline for the news story
    - conversation_ids: a list of strings representing the conversation ids for the tweets that support the headline

    Example output:
    [
        {{
            "headline": "Kamino Lend V2 is Live on Mainnet",
            "conversation_ids": ["1925525760103551210", "1925652788274413899", "1925605721892073504"]
        }}
        , {{
            "headline": "Alpenglow: Solana's new consensus protocol by Anza Research",
            "conversation_ids": ["1924491939040133515", "1925491131481035159"]
        }}
    ]

    Target around 10 headlines, but don't be afraid to include more or less based on the quality of the candidates.
    """
    # llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    llm = ChatOpenAI(model="gpt-4.1", temperature=0)
    response = llm.invoke(prompt)
    print(response.content)
    j = json.loads(response.content)

    headlines = pd.DataFrame(j)



    # clean_tweets[['username','text','valid','conversation_id']]
    tweet_url_searches = []
    check_tweets = []
    all_tweets = pd.DataFrame()
    upload_data = []
    for row in headlines.itertuples():
        print(f'== {row.headline}\n\n')
        conversation_ids = [int(i) for i in row.conversation_ids]
        text = '\n\n'.join(clean_tweets[clean_tweets.conversation_id.isin(conversation_ids)]['text'].tolist())
        query = generate_rag_search_query(text)
        # print(query)
        # print('\n\n')
        rag_tweets = rag_search_tweets_fn(query, 0, 0, [], 10)
        ids = set(conversation_ids + [int(x.id) for x in rag_tweets])
        ids = "'" + "', '".join([str(x) for x in ids]) + "'"
        query = f"""
            with t0 as (
                select conversation_id
                , author_id as original_author_id
                from tweets t
                where id = conversation_id
            )
            select t.*
            , coalesce(tu.name, tu.username, 'Unknown') as author
            , tu.username
            from tweets t
            join t0
                on t.conversation_id = t0.conversation_id
            left join twitter_users tu
                on t.author_id = tu.id
            where (t.id in ({ids}) or t.conversation_id in ({ids}))
                and t.author_id = t0.original_author_id
            order by t.created_at, t.id
        """
        cur_tweets = pg_load_data(query)
        cur_tweets['date'] = pd.to_datetime(cur_tweets['created_at'], unit='s').apply(lambda x: str(x)[:10])
        # cur_tweets['date'] = cur_tweets['created_at'].dt.strftime('%Y-%m-%d')
        cur_tweets = cur_tweets.groupby(['conversation_id','author_id','username','author','date']).agg({'text': '\n'.join}).reset_index()
        cur_tweets['headline'] = row.headline
        cur_tweets['original'] = (cur_tweets.conversation_id.isin(conversation_ids)).astype(int)
        all_tweets = pd.concat([all_tweets, cur_tweets])
        cur_check_tweets = []
        for tweet in cur_tweets.itertuples():
            # print(f'## Conversation ID: {tweet.conversation_id}\n\n## Author: {tweet.author_id}\n\n## Text: {tweet.text}\n\n\n')
            urls = re.findall(r'https://t\.co/\S+', tweet.text)
            for url in urls:
                r = requests.get(url)
                tweet_url_searches += [{
                    'headline': row.headline,
                    'url': r.url,
                    'status_code': r.status_code,
                    'text': BeautifulSoup(r.text, 'html.parser').text.strip()
                }]
                if r.url[:14] == 'https://x.com/' and not 'photos' in r.url and not 'video' in r.url:
                    tweet_id = r.url.split('/')[5]
                    cur_check_tweets.append({
                        'headline': row.headline,
                        'id': tweet_id,
                        'text': BeautifulSoup(r.text, 'html.parser').text.strip()
                    })
        if len(cur_check_tweets) > 0:
            pass
        # search the web
        prompt = f"""
        You are an expert research assistant skilled in transforming social media discussions and article headlines into precise and effective web search queries.

        Your task is to generate a **concise and specific search query** that can be used to find recent news articles or web content related to the Solana blockchain ecosystem.

        The search query should:
        - Focus on the key topic or event described in the headline and tweets
        - Include relevant project names, protocols, people, or themes mentioned
        - Be phrased as a simple string suitable for search engines (e.g., Google, Tavily)
        - Exclude unnecessary filler or vague language
        - Avoid hashtags, emojis, or @usernames unless highly relevant
        - Include any useful time framing (e.g., “May 2025”, “past week”) if it helps narrow the results

        ---

        Here is the **headline**:
        {row.headline}

        Here is the **tweet text**:
        {text}

        ---

        Output the optimized search query as a **single string only**, with no explanation.
        """
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        response = llm.invoke(prompt)
        query = 'Solana blockchain '+response.content
        web_search_results = tavily_client.search(query, search_depth="basic", include_answer=True, include_images=False, max_results=5, include_raw_content=True)
        # print(web_search_results)
        # print(web_search_results.keys())

        web_search_prompt = '## Web Search Reults\n\nUse these web search results to supplement the primary information. If they are helpful, use to help write the article. If they are not relevant, ignore them.\n\n'
        web_search_prompt = '**Summary**\n' + web_search_results['answer'] + '\n\n'
        web_search_prompt += '**Pages**\n\n'
        for r in web_search_results['results']:
            web_search_prompt += f'**{r["title"]}**\n'
            web_search_prompt += f'**URL:** {r["url"]}\n'
            web_search_prompt += f'**Summary:** {r["content"]}\n\n'
            if r['raw_content']:
                web_search_prompt += f'**Content:** {r["raw_content"]}\n\n'
        web_search_prompt += '\n\n'

        if not 'score' in cur_tweets.columns:
            cur_tweets = pd.merge(cur_tweets, clean_tweets[['conversation_id','score']], on='conversation_id', how='left')
        cur_tweets['score'] = cur_tweets['score'].fillna(0)
        cur_tweets = cur_tweets.sort_values('score', ascending=False)
        primary_tweet_prompt = ''
        for tweet in cur_tweets[cur_tweets.original == 1].itertuples():
            primary_tweet_prompt += f'Twitter URL: https://x.com/{tweet.username}/status/{tweet.conversation_id}\n\nTweet Date: {tweet.date}\n\nText: {tweet.text}\n\n\n'

        supplemental_tweet_prompt = '## Supplemental Tweets\n\nUse these tweets to supplement the primary information. If they are helpful, use to help write the article. If they are not relevant, ignore them.\n\n'
        for tweet in cur_tweets[cur_tweets.original == 0].itertuples():
            supplemental_tweet_prompt += f'Twitter URL: https://x.com/{tweet.username}/status/{tweet.conversation_id}\n\nTweet Date: {tweet.date}\n\nText: {tweet.text}\n\n\n'


        # search the web
        prompt = f"""
        You are an expert news writer focused on the crypto and blockchain ecosystem, especially the Solana blockchain.

        Your job is to synthesize multiple sources of information into a well-written, concise, and informative news article. Your article should read as if it were published on a professional crypto news site.

        You will be given:
        - A **proposed headline** (you may reword this slightly for clarity or style)
        - A list of **related tweets** discussing the topic
        - A set of **web search results** with relevant external content

        Your output must be a JSON object with the following fields:
        - headline: a string representing the headline for the news story
        - summary: a string representing the tl;dr for the news story
        - key_takeaways: a list of strings representing the key takeaways for the news story
        - sources: for each element in the key_takeaways list, include the url of the source. if multiple sources are used for a single key takeaway, include just the URL of the main source. this list should be equal to the length of the key_takeaways list. if the same source is used for multiple key takeaways, include it multiple times.
        However, if you do not feel like you have enough information to write a news article or the information is not worthy of a news article, return an empty JSON object.
        ---

        Guidelines:
        - Focus on accuracy, clarity, and conciseness
        - Do not speculate—only include information supported by the tweets or search results
        - You may reword and summarize for readability, but preserve factual integrity
        - Prioritize recent events or developments
        - Keep tone neutral and informative

        # Primary Information
        ## Proposed headline:
        Here is the **proposed headline**:
        {row.headline}

        ## Tweets:
        Here are the **primary tweets** the article should be based on:
        {primary_tweet_prompt}

        ---
        
        # Supplementary Information
        {web_search_prompt}

        {supplemental_tweet_prompt}

        ---
        
        Output the JSON object in the format above. Do not include any extra commentary or explanation.
        """

        # llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        llm = ChatOpenAI(model="gpt-4.1-mini", temperature=0)
        # llm = ChatOpenAI(model="gpt-4.1", temperature=0)
        response = llm.invoke(prompt)
        print(response.content)
        j = parse_json_from_llm(response.content, llm)
        j['score'] = round(clean_tweets[clean_tweets.conversation_id.isin(conversation_ids)].score.astype(float).max(), 2)
        j['timestamp'] = clean_tweets[clean_tweets.conversation_id.isin(conversation_ids)].created_at.max()

        upload_data += [j]

    upload_df = pd.DataFrame(upload_data)
    upload_df['n_days'] = n_days
    upload_df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    pg_upload_data(upload_df, 'news', 'append')

    tweet_url_searches_df = pd.DataFrame(tweet_url_searches)
    check_tweets_df = pd.DataFrame(check_tweets).rename(columns={'tweet_id': 'id'})
    check_tweets_df['id'] = check_tweets_df['id'].astype(int)
    # exclude if we already have the tweet in the df
    exists = all_tweets
    check_tweets_df = pd.merge(check_tweets_df, all_tweets[['conversation_id']].drop_duplicates(), on='conversation_id', how='left')
    ids = "'" + "', '".join([str(x) for x in check_tweets_df['id'].unique().tolist()]) + "'"
    query = f"select t.*, coalesce(tu.name, tu.username, 'Unknown') as author, tu.username as author_username from tweets t left join twitter_users tu on t.author_id = tu.id where t.id in ({ids}) or t.conversation_id in ({ids})"
    ap_tweets_df = pg_load_data(query)
    ap_tweets_df.id.unique()
    ap_tweets_df = pd.merge(ap_tweets_df, check_tweets_df, on='id', how='left')
    headlines_map = ap_tweets_df[['conversation_id','headline']].dropna().drop_duplicates()
    ap_tweets_df = ap_tweets_df.groupby(['conversation_id','author_id','author']).agg({'text': '\n'.join}).reset_index()
    check_tweets_df['headline'] = check_tweets_df['headline'].fillna('')
    all_tweets = pd.concat([all_tweets, check_tweets_df])

