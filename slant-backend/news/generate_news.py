import pandas as pd
from sqlalchemy import MetaData, Table, Column, Text, Integer, BigInteger, create_engine
from datetime import datetime
from langchain_openai import ChatOpenAI
from utils.db import pg_load_data, pg_upsert_data, POSTGRES_ENGINE
from ai.tools.twitter.rag_search_tweets import rag_search_tweets_fn
from ai.tools.slant.rag_search_projects import rag_search_projects_from_prompt
import json
import re
import requests
from bs4 import BeautifulSoup
from tavily import TavilyClient
from constants.keys import TAVILY_API_KEY
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from utils.db import pg_upload_data
from datetime import timedelta
from utils.utils import clean_project_tag

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

def parse_date(entry):
    # Replace with actual key (e.g., entry.get('metadata', {}).get('date_published'))
    for key in ['date_published', 'published', 'date']:
        date_str = entry.get('metadata', {}).get(key)
        if date_str:
            try:
                return datetime.fromisoformat(date_str)
            except ValueError:
                pass  # Handle other formats if needed
    return None

def generate_news(clean_tweets: pd.DataFrame, n_days: int):
    tavily_client = TavilyClient(api_key=TAVILY_API_KEY)
    saved_web_searches = []

    #######################################
    #     Generate Original Headlines     #
    #######################################
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

    Common mistakes to avoid:
    - Tweets that are analyzing current events, but not news stories themselves (often these are just the author's opinion)

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

    Make sure your information and timeframe is accurate, being careful to not make up or mis-represent any information. Details matter.
    """
    # llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    llm = ChatOpenAI(model="gpt-4.1", temperature=0)
    # llm = ChatAnthropic(model="claude-3-5-sonnet-20240620", temperature=0)
    response = llm.invoke(prompt)
    # print(response.content)
    j = parse_json_from_llm(response.content, llm)
    headlines = pd.DataFrame(j)
    print('Headlines:')
    print(headlines)

    #####################################
    #     Generate Headlines 1-by-1     #
    #####################################
    tweet_url_searches = []
    all_tweets = pd.DataFrame()
    upload_data = []
    for row in headlines.itertuples():
        print(f'== {row.headline}\n\n')

        ################################
        #     Query Similar Tweets     #
        ################################
        conversation_ids = [int(i) for i in row.conversation_ids]
        text = ''
        for tweet in clean_tweets[clean_tweets.conversation_id.isin(conversation_ids)].itertuples():
            text += f'## Tweet Author:\n{tweet.username}\n## Tweet Text:\n{tweet.text}\n\n\n'
        query = generate_rag_search_query(text)
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
        similar_tweets = pg_load_data(query)
        similar_tweets['date'] = pd.to_datetime(similar_tweets['created_at'], unit='s').apply(lambda x: str(x)[:10])
        similar_tweets = similar_tweets.groupby(['conversation_id','author_id','username','author','date']).agg({'text': '\n'.join}).reset_index()
        similar_tweets['headline'] = row.headline
        similar_tweets['original'] = (similar_tweets.conversation_id.isin(conversation_ids)).astype(int)
        similar_tweets = similar_tweets.sort_values('original', ascending=False)


        ################################
        #     Extract Project Name     #
        ################################
        tweet_text = ''
        for tweet in similar_tweets.itertuples():
            if tweet.original == 1:
                tweet_text += f'**Primary Tweet:**\n'
            tweet_text += f'## Tweet Author:\n{tweet.username}\n## Tweet Text:\n{tweet.text}\n\n\n'
        # print(tweet_text)

        prompt = f"""
        You are an expert at ingesting tweets about the solana blockchain ecosystem and generating a project name that the tweets are about. You will be given a primary tweet and a list of supplemental tweets. Weight the primary tweet more heavily.

        Here is the list of tweets:
        {tweet_text}

        Your job is to return a string representing the project name that the tweets are about.
        If you cannot determine the project name, return an empty string.

        Output the project name as a string only, with no explanation.
        """
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        response = llm.invoke(prompt)
        project_name = response.content.replace('"', '')
        project_name = clean_project_tag(project_name)
        print(f'Project Name: {project_name}')


        #####################################
        #     Extract Links from Tweets     #
        #####################################
        all_tweets = pd.concat([all_tweets, similar_tweets])
        quoted_tweets = []
        for tweet in similar_tweets.itertuples():
            urls = re.findall(r'https://t\.co/\S+', tweet.text)
            for url in urls:
                try:
                    r = requests.get(url)
                    tweet_url_searches += [{
                        'headline': row.headline,
                        'url': r.url,
                        'status_code': r.status_code,
                        'text': BeautifulSoup(r.text, 'html.parser').text.strip()
                    }]
                    if r.status_code == 200:
                        cur = {
                            'url': r.url,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'project': project_name,
                            'user_message_id': '',
                            'search_query': query,
                            'base_url': r.url,
                            'text': BeautifulSoup(r.text, 'html.parser').text.strip()
                        }
                        saved_web_searches += [cur]
                    elif r.url[:14] == 'https://x.com/' and not 'photos' in r.url and not 'video' in r.url and '/status/' in r.url:
                        tweet_id = r.url.split('/status/')[1]
                        tweet_id = int(tweet_id.split('/')[0].split('?')[0])
                        quoted_tweets.append({
                            'headline': row.headline,
                            'id': tweet_id,
                            'text': BeautifulSoup(r.text, 'html.parser').text.strip()
                        })
                except Exception as e:
                    print(f'Error getting url {url}: {e}')
                    continue
        if len(quoted_tweets) > 0:
            # load any tweets that were referenced in the original tweets
            quoted_tweets_df = pd.DataFrame(quoted_tweets)
            ids = "'" + "', '".join([str(x) for x in quoted_tweets_df['id'].unique().tolist()]) + "'"
            exlude_ids = "'" + "', '".join([str(x) for x in similar_tweets['conversation_id'].unique().tolist()]) + "'"
            query = f"""
            select t.*
            , coalesce(tu.name, tu.username, 'Unknown') as author
            , tu.username
            from tweets t
            left join twitter_users tu
                on t.author_id = tu.id
            where (t.id in ({ids}) or t.conversation_id in ({ids}))
                and t.conversation_id not in ({exlude_ids})
            order by t.created_at, t.id
            """
            new_tweets = pg_load_data(query)
            new_tweets['date'] = pd.to_datetime(new_tweets['created_at'], unit='s').apply(lambda x: str(x)[:10])
            new_tweets = new_tweets.groupby(['conversation_id','author_id','username','author','date']).agg({'text': '\n'.join}).reset_index()
            new_tweets['headline'] = row.headline
            new_tweets['original'] = 0
            # print('Loaded', len(new_tweets), 'quoted tweets')
            similar_tweets = pd.concat([similar_tweets, new_tweets])


        #######################################
        #     Generate Web Search Results     #
        #######################################
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
        query = 'Solana blockchain '+response.content.replace('"', '')
        web_search_results = tavily_client.search(query, search_depth="basic", include_answer=True, include_images=False, max_results=5, include_raw_content=True)


        ############################################
        #     Generate Prompt for News Article     #
        ############################################
        web_search_prompt = '## Web Search Results\n\nUse these web search results to supplement the primary information. If they are helpful, use to help write the article. If they are not relevant, ignore them.\n\n'
        web_search_prompt = '**Summary**\n' + web_search_results['answer'] + '\n\n'
        web_search_prompt += '**Pages**\n\n'
        cutoff_date = datetime.now() - timedelta(days=(n_days * 2) + 14)
        for r in web_search_results['results']:
            date = parse_date(r)
            # print('date:', date)
            # save the web search results to db
            cur = {
                'url': r['url'],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'project': project_name,
                'user_message_id': '',
                'search_query': query,
                'base_url': r['url'].split('/')[2],
                'text': r['raw_content']
            }
            saved_web_searches += [cur]
            if (date and date > cutoff_date) or not date:
                web_search_prompt += f'**{r["title"]}**\n'
                web_search_prompt += f'**URL:** {r["url"]}\n'
                # web_search_prompt += f'**Date:** {date.strftime("%Y-%m-%d")}\n'
                web_search_prompt += f'**Summary:** {r["content"]}\n\n'
                if r['raw_content']:
                    web_search_prompt += f'**Content:** {r["raw_content"]}\n\n'
        web_search_prompt += '\n\n'

        if not 'score' in similar_tweets.columns:
            similar_tweets = pd.merge(similar_tweets, clean_tweets[['conversation_id','score']], on='conversation_id', how='left')
        similar_tweets['score'] = similar_tweets['score'].fillna(0)
        similar_tweets = similar_tweets.sort_values('score', ascending=False)
        primary_tweet_prompt = ''
        for tweet in similar_tweets[similar_tweets.original == 1].itertuples():
            primary_tweet_prompt += f'Twitter URL: https://x.com/{tweet.username}/status/{tweet.conversation_id}\n\nTweet Date: {tweet.date}\n\nText: {tweet.text}\n\n\n'

        supplemental_tweet_prompt = '## Supplemental Tweets\n\nUse these tweets to supplement the primary information. If they are helpful, use to help write the article. If they are not relevant, ignore them.\n\n'
        for tweet in similar_tweets[similar_tweets.original == 0].itertuples():
            supplemental_tweet_prompt += f'Twitter URL: https://x.com/{tweet.username}/status/{tweet.conversation_id}\n\nTweet Date: {tweet.date}\n\nText: {tweet.text}\n\n\n'
        
        query = f"""
        select distinct k.name, k.username, k.description, case when p.tags::text ilike '%news%' then 1 else 0 end as news_tag
        from twitter_kols k
        join tweets t
            on k.id = t.author_id
        left join projects p
            on k.associated_project_id = p.id
        where t.id in ({ids})
            and not k.username in ('solana','DegenerateNews')
        limit 10
        """
        authors = pg_load_data(query)
        project_query = row.headline + '\n\n' + '\n'.join([f'{a.name} - {a.username} - {a.description}' for a in authors.itertuples()])
        projects = rag_search_projects_from_prompt(project_query, 5)
        # for p in projects:
        #     print(p)
        projects = ' - ' + ' - '.join([p.name for p in projects])


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
        - sources: for each element in the key_takeaways list, include the url of the source. if multiple sources are used for a single key takeaway, include just the URL of the main source. this list should be equal to the length of the key_takeaways list. if the same source is used for multiple key takeaways, include it multiple times. We prefer Twitter (X) URLs over other sources.
        - projects: a list of strings representing the projects mentioned in the news story. you MUST choose from the following list. if none of the projects are mentioned, return an empty list.
            {projects}
        - tag: a single string representing the tag for the news story. For the tag, choose between the following:
            - "DeFi" (decentralized finance like lending, borrowing, trading, etc.)
            - "NFTs"
            - "Memecoins"
            - "Gaming"
            - "DePIN" (decentralized physical infrastructure like Helium, Render, Hivemapper, etc.)
            - "Community" (events, groups, real world / IRL, etc.)
            - "Payments" (stablecoins, payment networks, etc.)
            - "Infrastructure" (blockchain infrastructure, validators, etc.)
            - "Legal" (regulations, partnerships, SEC, etc.)
            - "DAOs" (decentralized autonomous organizations, governance, etc.)
            - "Other" (if the news story does not fit into any of the above categories)
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
        # llm = ChatOpenAI(model="gpt-4.1-mini", temperature=0)
        llm = ChatOpenAI(model="gpt-4.1", temperature=0)
        response = llm.invoke(prompt)
        # print(response.content)
        j = parse_json_from_llm(response.content, llm)
        j['score'] = round(clean_tweets[clean_tweets.conversation_id.isin(conversation_ids)].score.astype(float).max(), 2)
        j['timestamp'] = clean_tweets[clean_tweets.conversation_id.isin(conversation_ids)].created_at.max()
        j['original_tweets'] = clean_tweets[clean_tweets.conversation_id.isin(conversation_ids)].sort_values('score', ascending=False).conversation_id.tolist()

        upload_data += [j]

    upload_df = pd.DataFrame(upload_data)
    upload_df['n_days'] = n_days
    upload_df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    upload_df['original_tweets'] = upload_df['original_tweets'].apply(lambda x: json.dumps(x))
    upload_df['sources'] = upload_df['sources'].apply(lambda x: json.dumps(x))
    upload_df['key_takeaways'] = upload_df['key_takeaways'].apply(lambda x: json.dumps(x))
    upload_df['projects'] = upload_df['projects'].apply(lambda x: json.dumps(x))
    upload_df[['headline','projects','tag']].to_csv('~/Downloads/tmp-4.csv', index=False)
    pg_upload_data(upload_df, 'news', 'append')

    saved_web_searches_df = pd.DataFrame(saved_web_searches).drop_duplicates(subset=['url'], keep='last').dropna()
    saved_web_searches_df = saved_web_searches_df[saved_web_searches_df.text != '']
    saved_web_searches_df['user_message_id'] = 'test'
    saved_web_searches_df['text'] = saved_web_searches_df['text'].apply(lambda x: x[:35000])
    # Remove NULL characters from all string columns
    for col in saved_web_searches_df.select_dtypes(include=['object']).columns:
        saved_web_searches_df[col] = saved_web_searches_df[col].astype(str).str.replace('\x00', '')
    for c in saved_web_searches_df.columns:
        # print(c, len(saved_web_searches_df[saved_web_searches_df[c] == ''][c].unique()))
        # print(c, len(saved_web_searches_df[saved_web_searches_df[c].isna()]))
        print(c, len(saved_web_searches_df[saved_web_searches_df[c] == None]))
    # saved_web_searches_df.search_query.unique()
    saved_web_searches_df.to_csv('~/Downloads/tmp-6.csv', index=False)
    saved_web_searches_df.count()
    engine = create_engine(POSTGRES_ENGINE)
    metadata = MetaData()
    table = Table(
        "web_searches", metadata,
        Column("url", Text, primary_key=True),
        Column("timestamp", Integer),
        Column("project", Text),
        Column("project_id", BigInteger),
        Column("user_message_id", Text),
        Column("search_query", Text),
        Column("base_url", Text),
        Column("text", Text)
    )
    pg_upsert_data(saved_web_searches_df, table, engine, ['url'])
