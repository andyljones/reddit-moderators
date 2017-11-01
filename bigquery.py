#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
 - Install the bigquery library with "pip install google-cloud-bigquery"

 - Go to the Google Cloud console: https://console.cloud.google.com
 - Create a project ('reddit-network' or whatever)
 - Create service account credentials with read privileges: https://console.cloud.google.com/apis/credentials
 - Download the credentials to the local dir and point the CREDENTIALS variable at them
 - Add the 'BigQuery Job User' role to the service account: https://console.cloud.google.com/iam-admin/iam
 
 - BigQuery Python API: https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/usage.html#tables
 - BigQuery API: https://cloud.google.com/bigquery/docs/tables
 - SQL docs: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
 - Parameterized query docs: https://cloud.google.com/bigquery/docs/parameterized-queries
 - Reddit table schema: https://bigquery.cloud.google.com/table/fh-bigquery:reddit_comments.all

 - Install spacy
 - Download the english model: `python -m spacy download en`
"""

import scipy as sp
import spacy
import pandas as pd
from tqdm import tqdm
from google.cloud import bigquery

PROJECT = 'reddit-network'
CREDENTIALS = 'reddit-network-f00ede7e73d0.json'

def client():
    return bigquery.Client.from_service_account_json(CREDENTIALS, project=PROJECT)

def comments_table(name='2005'):
    return client().dataset('reddit_comments').table('all')

def job(query, config, max_bytes=1e9):
    config.use_legacy_sql = False
    config.maximum_bytes_billed = int(max_bytes)
    
    job = client().query(query=query, job_config=config)
    iterator = job.result()

    rows = []
    for row in iterator:
        rows.append(row.values())
        
    columns = [c.name for c in iterator.schema]
    return pd.DataFrame(rows, None, columns)
    

def all_comments(table, subreddit, **kwargs):
    query = """select body, author, created_utc, parent_id, subreddit, score
               from `fh-bigquery.reddit_comments.{}`
               where (subreddit = @subreddit)
               """.format(table)
    
    config = bigquery.QueryJobConfig()
    config.query_parameters = (bigquery.ScalarQueryParameter('subreddit', 'STRING', subreddit))
    
    return job(query, config, **kwargs)

def sample_comments(table, size=10, **kwargs):
    query = """select subreddit, array_agg(struct(body, id) order by rand() desc limit @size) as agg
               from `fh-bigquery.reddit_comments.{}`
               group by subreddit""".format(table)
        
    config = bigquery.QueryJobConfig()
    config.query_parameters = (bigquery.ScalarQueryParameter('size', 'INT64', size),)
    
    result = job(query, config, **kwargs)
    
    # This could definitely be done faster :/
    samples = pd.concat(result.set_index('subreddit')['agg'].apply(pd.DataFrame).to_dict())
    
    return samples

def lemmatize(samples, nlp):
    #TODO: This could be sped up with nlp.pipe
    strings = samples.body.add(' | ').groupby(level=0).sum()
    
    indices = []
    for i, s in enumerate(tqdm(strings)):
        indices.extend([(i, t.lemma) for t in nlp(s)])
    indices = sp.array(indices)
    
    rows = indices[:, 0]
    cols = indices[:, 1]
    vals = sp.ones_like(rows)
    indicators = sp.sparse.csc_matrix((vals, (rows, cols)), (len(samples), len(nlp.vocab.strings)+1))
    
    return strings.index.tolist(), indicators
    
def example():
    nlp = spacy.load('en')
    
    samples = sample_comments('2009')
    subreddits, indicators = lemmatize(samples, nlp)