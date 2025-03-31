--
-- PostgreSQL database dump
--

-- Dumped from database version 16.4 (Debian 16.4-1.pgdg120+2)
-- Dumped by pg_dump version 16.8 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: projects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.projects (
    id bigint NOT NULL,
    name text NOT NULL,
    parent_project_id integer,
    description text,
    ecosystem text,
    tags json
);


--
-- Name: projects_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.projects_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: projects_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.projects_id_seq OWNED BY public.projects.id;


--
-- Name: referenced_tweets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.referenced_tweets (
    id bigint NOT NULL,
    referenced_tweet_id bigint NOT NULL,
    referenced_tweet_type text NOT NULL,
    author_id bigint NOT NULL
);


--
-- Name: tweets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tweets (
    id bigint NOT NULL,
    conversation_id bigint,
    author_id bigint,
    created_at integer,
    text text,
    retweet_count integer,
    reply_count integer,
    like_count integer,
    quote_count integer,
    impression_count integer
);


--
-- Name: twitter_kols; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.twitter_kols (
    id bigint,
    account_type text,
    tracking boolean,
    username text,
    name text,
    description text,
    followers_count integer,
    associated_project_id bigint
);


--
-- Name: twitter_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.twitter_users (
    id bigint NOT NULL,
    name text NOT NULL,
    username text NOT NULL
);


--
-- Name: projects id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.projects ALTER COLUMN id SET DEFAULT nextval('public.projects_id_seq'::regclass);


--
-- Name: projects projects_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_pkey PRIMARY KEY (id);


--
-- Name: referenced_tweets referenced_tweets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.referenced_tweets
    ADD CONSTRAINT referenced_tweets_pkey PRIMARY KEY (id, referenced_tweet_id, referenced_tweet_type, author_id);


--
-- Name: tweets tweets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tweets
    ADD CONSTRAINT tweets_pkey PRIMARY KEY (id);


--
-- Name: twitter_users twitter_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.twitter_users
    ADD CONSTRAINT twitter_users_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

