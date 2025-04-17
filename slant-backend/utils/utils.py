import re
import os
import json
import pandas as pd
from datetime import datetime
from constants.constant import DEBUG_MODE

def get_timestamp():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def get_base_path():
    return re.split('utils', os.path.dirname(os.path.abspath(__file__)))[0]

def read_csv(path):
    return pd.read_csv(path)

def write_csv(df, path):
    df.to_csv(path, index=False)

def log(message):
    if DEBUG_MODE:
        print(message)

def clean_project_tag(tag: str) -> str:
    try:
        tag = tag.lower()
        phrases = [' ','.gg', '$', '_']
        for phrase in phrases:
            tag = tag.replace(phrase, '')
        phrases = ['jupiter lfg','famous fox']
        for phrase in phrases:
            if tag[:len(phrase)] == phrase:
                tag = phrase
        phrases = ['finance','fi','protocol','network']
        for phrase in phrases:
            if tag[-len(phrase):] == phrase:
                tag = tag.replace(phrase, '')
        tag = tag.replace('.', '')
        d = {}
        if tag in d.keys():
            tag = d[tag]
        return tag
    except Exception as e:
        print(f"Error cleaning project tag: {e}")
        return tag

def clean_token_name(token: str) -> str:
    try:
        token = token.lower()
        phrases = [' ','$', '_']
        for phrase in phrases:
            token = token.replace(phrase, '')
        phrases = []
        for phrase in phrases:
            if token[:len(phrase)] == phrase:
                token = phrase
        phrases = []
        for phrase in phrases:
            if token[-len(phrase):] == phrase:
                token = token.replace(phrase, '')
        token = token.replace('.', '')
        d = {}
        if token in d.keys():
            token = d[token]
        return token
    except Exception as e:
        print(f"Error cleaning token name: {e}")
        return tag

def clean_token_names(tokens: str) -> list[str]:
    try:
        tokens = tokens.replace('```json', '').replace('```', '').strip()
        tokens = json.loads(tokens)
        tokens = [clean_token_name(x) for x in tokens]
        tokens = [x for x in tokens if not x in ['solana','sol']]
        return tokens
    except Exception as e:
        print(f"Error cleaning token names: {e}")
        return []

def clean_project_tags(tags: str) -> list[str]:
    try:
        tags = tags.replace('```json', '').replace('```', '').strip()
        tags = json.loads(tags)
        tags = [clean_project_tag(x) for x in tags]
        tags = [x for x in tags if not x in ['solana']]
        return tags
    except Exception as e:
        print(f"Error cleaning project tags: {e}")
        return []