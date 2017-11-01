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
"""

import pandas as pd
import uuid
from google.cloud import bigquery

PROJECT = 'reddit-network'
CREDENTIALS = 'reddit-network-f00ede7e73d0.json'

QUERY = """
select  body, author, created_utc, parent_id, subreddit, score
from `fh-bigquery.reddit_comments.{}`
where (subreddit = @subreddit)
limit @lim
"""

def client():
    return bigquery.Client.from_service_account_json(CREDENTIALS, project=PROJECT)

def comments_table(name='2005'):
    return client().dataset('reddit_comments').table('all')

def query(table, subreddit, limit=100, name='default', max_bytes=1e9):
    params = (bigquery.ScalarQueryParameter('subreddit', 'STRING', subreddit),
              bigquery.ScalarQueryParameter('lim', 'INT64', limit))
    
    job = client().run_async_query(
                job_name='{}-{}'.format(name, str(uuid.uuid4())),
                query=QUERY.format(table),
                query_parameters=params)
    job.use_legacy_sql = False
    job.maximum_bytes_billed = int(max_bytes)
    
    job.begin()
    job.result()
    
    job.destination.reload()
    columns = [c.name for c in job.destination.schema]
    rows = []
    for row in job.destination.fetch_data():
        rows.append(row)
        
    result = pd.DataFrame(rows, None, columns)
    
    return result

def example():
    return query('2005', 'reddit.com')