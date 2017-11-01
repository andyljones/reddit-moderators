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

PROJECT = 'reddit-network-184710' #unique project id not name
CREDENTIALS = 'reddit-network-774059619c28.json'

QUERY = """
select  body, author, created_utc, id, link_id, parent_id, subreddit, score
from `fh-bigquery.reddit_comments.{}`
where (subreddit = @subreddit)
limit @lim
"""

def client():
    return bigquery.Client.from_service_account_json(CREDENTIALS, project=PROJECT)

def comments_table(name='2005'):
    return client().dataset('reddit_comments').table('all')

def query(table, subreddit, limit=100, name='default', max_bytes=1e9):
    config = bigquery.QueryJobConfig()
    config.query_parameters = (bigquery.ScalarQueryParameter('subreddit', 'STRING', subreddit),
                               bigquery.ScalarQueryParameter('lim', 'INT64', limit))
    config.use_legacy_sql = False
    config.maximum_bytes_billed = int(max_bytes)
    
    job = client().query(
                query=QUERY.format(table),
                job_config=config,
                job_id_prefix=name)
    
    iterator = job.result()

    rows = []
    for row in iterator:
        rows.append(row.values())
        
    columns = [c.name for c in iterator.schema]
    result = pd.DataFrame(rows, None, columns)
    
    return result

def example():
    return query('2005', 'reddit.com')

ex = example()

