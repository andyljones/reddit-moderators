#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 29 08:10:27 2017

@author: andyjones
"""

import json
import praw
import pandas as pd
import logging
import logging.handlers
import os
from datetime import datetime

USER_AGENT = 'test script for /u/bluecoffee'

OUTPUT_DIR = 'output'
LOGGING_DIR = 'logging/logs.txt'

def api():
    if not hasattr(api, '_cache'):
        with open('credentials.json', 'rb') as f:
            credentials = json.load(f)
        
        api._cache = praw.Reddit(**credentials, user_agent=USER_AGENT)
    
    return api._cache

def subreddits(limit=100):
    return list(api().subreddits.popular(limit=limit))

def moderators(subreddit):
    # List argument case: recurse on each element of the list, then combine the results
    if isinstance(subreddit, list):
        results = {}
        for s in subreddit:
            logging.info('Fetching moderators for {}'.format(s))
            results[s.display_name] = moderators(s)
        results = pd.concat(results)
        
        return results
        
    # Non-list argument case: fetch the mod details for that subreddit
    results = []
    for m in subreddit.moderator():
        result = {('detail', 'name'): m.name,
                  ('detail', 'date'): pd.to_datetime(m.date, unit='s')}
        for p in m.mod_permissions:
            result[('permission', p)] = True
        
        results.append(result)
    results = pd.DataFrame(results)
    results.columns = pd.MultiIndex.from_tuples(results.columns)
    results = results.sort_index(axis=1)
    
    return results

def scrape():
    start_time = datetime.now()
    logging.info('Running...')
    
    # Probably want to have a fixed list of subreddits instead of this, as the 
    # list of most popular subreddits will change over time.
    subs = subreddits(limit=10)
    
    logging.info('Fetching moderators for {} subs'.format(len(subs)))
    mods = moderators(subs) 
    mods[('meta', 'start_time')] = start_time
    
    path = os.path.join(OUTPUT_DIR, '{:%Y-%m-%d/%H:%M:%S}.pkl'.format(start_time))
    logging.info('Saving to "{}"'.format(path))
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mods.to_pickle(path)
    logging.info('Saved.')
    
def load():
    paths = []
    for dirname, _, filenames in os.walk(OUTPUT_DIR):
        paths.extend([os.path.join(dirname, fn) for fn in filenames])
    paths = sorted(paths)
    
    results = []
    for p in paths:
        df = pd.read_pickle(p)
        results.append(df)
    results = pd.concat(results)
    
    results['permission'] = results['permission'].fillna(False)
    
    results = (results
                   .reset_index(level=1, drop=True)
                   .set_index([('detail', 'name'), 
                               ('meta', 'start_time')], append=True)
                   .sort_index(0))
    results.index.names = ['' for _ in results.index.names]
    
    return results
    
def _configure_logging():
    os.makedirs(os.path.dirname(LOGGING_DIR), exist_ok=True)
    file_handler = logging.handlers.TimedRotatingFileHandler(LOGGING_DIR, 
                                                             when='W0', 
                                                             backupCount=7)
    
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.StreamHandler(),
                              file_handler])
    
_configure_logging()