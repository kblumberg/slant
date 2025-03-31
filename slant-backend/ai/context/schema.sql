
--
-- Name: projects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.projects (
    id bigint NOT NULL, -- primary key auto-incremented
    name text NOT NULL, -- name of the project
    parent_project_id integer, -- id of the parent project (if exists)
    description text, -- description of the project
    ecosystem text, -- ecosystem of the project (usually "solana")
    tags json -- tags of the project (e.g. ["defi", "nft", "gaming"])
    score float -- importance score of the project (0-100)
);

--
-- Name: referenced_tweets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.referenced_tweets (
    id bigint NOT NULL, -- id of the tweet
    referenced_tweet_id bigint NOT NULL, -- id of the referenced tweet
    referenced_tweet_type text NOT NULL, -- type of the referenced tweet (e.g. "retweeted", "replied_to", "quoted")
    author_id bigint NOT NULL -- id of the author
);


--
-- Name: tweets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tweets (
    id bigint NOT NULL, -- id of the tweet
    conversation_id bigint, -- id of the conversation
    author_id bigint, -- id of the author
    created_at integer, -- unix timestamp in seconds
    text text, -- text of the tweet
    retweet_count integer, -- number of retweets
    reply_count integer, -- number of replies
    like_count integer, -- number of likes
    quote_count integer, -- number of quotes
    impression_count integer -- number of impressions
);


--
-- Name: twitter_kols; Type: TABLE; Schema: public; Owner: -
--
-- text fields are case sensitive, so use lower() when querying or ilike
CREATE TABLE public.twitter_kols (
    id bigint, -- id of the twitter kol
    account_type text, -- type of the account (e.g. "project", "influencer"). to exclude projects and include just people, use account_type != 'project'
    tracking boolean, -- whether the kol is being tracked
    username text, -- username of the kol. case sensitive, so use lower() when querying or ilike
    name text, -- name of the kol. case sensitive, so use lower() when querying or ilike
    description text, -- description of the kol. case sensitive, so use lower() when querying or ilike
    followers_count integer, -- number of followers
    associated_project_id bigint, -- id of the project associated with the kol
    score float -- importance score of the kol (0-100)
);


--
-- Name: twitter_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.twitter_users (
    id bigint NOT NULL, -- id of the twitter user
    name text NOT NULL, -- name of the user
    username text NOT NULL -- username of the user
);
