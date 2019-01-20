
# coding: utf-8

# In[1]:


"""
This script pulls iTunes reviews for the NPR News/NPR App (324906251) collected on
http://dev-sandbox.npr.org/dperry/reviews/apps/324906251.json

It requests the saved iTunes reviews in JSON format, turns them into a csv, then saves that CSV file to BigQuery.
Schema for the BigQuery table is automatically detected.

For documentation and code on how the iTunes reviews are grabbed, see https://gitlab.com/perrydc/reviews.
"""


# In[2]:


import requests
import pandas as pd
import json


#pip install --upgrade google-cloud-bigquery
#pip install pandas,google-api-helper, requests 

# Imports the Google Cloud client library
#https://pypi.python.org/pypi/google-api-helper/0.2.2
from googleapi import GoogleApi


# In[3]:


# Pulling in reviews from the sandbox
resp = requests.get("http://dev-sandbox.npr.org/dperry/reviews/apps/324906251.json")


# In[4]:


# Creating pandas dataframe, setting new index & renaming date column (for ease of use with Mode Analytics),
# and adding sentiment polarity column; this isn't used in anything yet, but could be useful.

df = pd.DataFrame(resp.json()['reviews'])
df = df.set_index('id')
df = df.rename(columns={'date': 'created_at'})
#df['sentiment'] = [ TextBlob(df['content'][i]).sentiment.polarity for i in range(len(df))]

# Most recent first
df = df.sort_values(by=['created_at'], ascending=False)
df.head()


# In[5]:


# Writing to CSV
with open('iTunesReviews.csv', 'w') as outfile:
            df.to_csv(outfile, index=True)

# In[6]:


""" BIGQUERY
    Make sure that JSON key is in correct location and that bash profile is modified to have the path set.
    Uses two different libraries / python wrappers, one to delete and recreate the table,
    the other to load data with the schema auto-detected
    https://cloud.google.com/bigquery/docs/authentication/
"""

def del_recreate_bq(projectId, datasetId, tableId):
    bigquery = GoogleApi('bigquery', 'v2', [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/devstorage.full_control'
    ])

    ##CHANGE THE PATH & JSON KEY HERE TO MATCH YOURS
    bigquery.with_service_account_file('Midd-d3b73b87b0cf.json')
    try:
        service_request = bigquery.tables().delete(projectId=projectId, datasetId=datasetId, tableId=tableId)
        service_request.execute()
    except:
        print('No table to delete')

    body = {
      "tableReference":
      {
        "projectId": projectId,
        "datasetId": datasetId,
        "tableId": tableId
      }
    }

    try:
        service_request = bigquery.tables().insert(projectId=projectId, datasetId=datasetId, body=body)
        service_request.execute()
        print('Success, created table ' + tableId)
    except:
        print('Unable to create table ' + tableId)
    return None

#Modified from google documentation
def load_data_from_file(dataset_id, table_id, source_file_name):
    # Instantiates a client
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    with open(source_file_name, 'rb') as source_file:
        # This example uses CSV, but you can use other formats.
        # See https://cloud.google.com/bigquery/loading-data
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = 'text/csv'
        job_config.autodetect = True
        job = bigquery_client.load_table_from_file(
            source_file, table_ref, job_config=job_config)

    job.result()  # Waits for job to complete

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))

# In[7]:

##########Change for Dataset#########
projectId = "midd-194719"
datasetId = "hippo"
tableIdNPRApp = "iTunesReviews"
#####################################

#Deleteing and recreating the bq tables
del_recreate_bq(projectId, datasetId, tableIdNPRApp)

#Has to Be imported beforehand
from google.cloud import bigquery

#Loading data from the NPR App and Labels to their tables
load_data_from_file(datasetId, tableIdNPRApp, 'iTunesReviews.csv')
