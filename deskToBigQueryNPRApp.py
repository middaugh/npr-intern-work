
# coding: utf-8

# In[1]:


"""This script pull's data from Desk's API.
Required: Working Google BigQuery Service account and JSON Key on machine
(see https://cloud.google.com/bigquery/docs/authentication/ for more information; also Nick DePrey and Demian)
"""
import requests
import pandas as pd
import json
import configparser
from requests_oauthlib import OAuth1

# Imports the Google Cloud client library
#%pip install --upgrade google-cloud-bigquery
from google.cloud import bigquery
from googleapi import GoogleApi


# In[2]:


class Desk():
    '''
    This class includes all the methods necessary to pull data from Desk.com.

    '''
    def __init__(self, sitename, auth):
        self.sitename = sitename
        self.auth = auth

    def get_data(self, request_url):
        requests.packages.urllib3.disable_warnings()
        resp = requests.get(self.sitename + request_url, auth=self.auth, verify=False ) #verify=false necessary if running off server
        return resp.json()

    #As it stands now there are so few cases that it makes sense to just pull all of them each time. Should this
    #this change incorporate the script from pullAllDeskCases to pull based off time
    #(would require some tweaking of update cases)
    def cycle_pages(self, request_url, update=False):
        self.all_data = []
        data = self.get_data(request_url)
        while data['_links']['next'] != None:
            self.all_data.extend(data['_embedded']['entries'])
            request_url = data['_links']['next']['href']
            #For unknown reasons, when authenticating via OAuth1, ':' is replaced by '%3A' -- reverses that
            if '%3A' in request_url:
                request_url = request_url.replace('%3A', ':')
            data= self.get_data(request_url)
        self.all_data.extend(data['_embedded']['entries'])

    #Grabs the JSON data and returns a properly formatted pandas dataframe with only the columns you specify
    def get_df(self, data, update=None, fields=['id', 'blurb', 'labels', 'created_at', 'subject'] ):
        self.fields = fields
        json_data = json.dumps(data)
        df = pd.read_json(json_data)
        #selects only the values you are interested in; adjust for Audience Relations
        df_functional = df[:][fields].copy()
        self.df = df_functional

    #Writes the pandas data frame to a csv file
    def load_csv(self, fname):
        with open(fname, 'w') as outfile:
            self.df.to_csv(outfile, index=False)

    #Combines cycle_pages, get_df, and load_csv into one function
    def pull_save_cases(self, request_url=None, fname=None, fields=None):
        try:
            print("Trying to pull cases from Desk.com")
            self.cycle_pages(request_url)
            if fields != None:
                try:
                    self.get_df(self.all_data, fields=fields)
                except:
                    print('Fields Invalid, pulling default fields: ')
                    self.get_df(self.all_data)
                    print(str(self.fields))

            else:
                self.get_df(self.all_data)
            self.load_csv(fname)
            print('You have successfully saved {} cases to {}'.format(len(self.df), fname))
        except Exception as e: print(e)
            




# In[3]:


def del_recreate_bq(projectId, datasetId, tableId):
    bigquery = GoogleApi('bigquery', 'v2', [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/devstorage.full_control'
    ])

    #NEED TO UPDATE TO YOUR JSON  KEY
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


# In[4]:


#I wasn't able to get it working, but if you can get pandas.to_gbq to work would be much cleaner!
#See https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_gbq.html


# In[5]:


"""Can take configuration file with data for OAuth1, or else set yourself
If setting yourself, you can just pass in a username and password as such
import getpass
username = input('Enter username:')
password = getpass.getpass()
auth = username, password
"""
sitename = 'https://help.npr.desk.com'

npr_app_data = 'deskCasesNPRApp.csv'
labels_data = 'deskCasesLabels.csv'

#https://docs.python.org/3/library/configparser.html
config = configparser.ConfigParser(allow_no_value=True)
config.read('desk_OAuth1.ini')
auth = OAuth1(config['DESK']['app_key'],
              config['DESK']['app_secret'],
              config['DESK']['oauth_token'],
              config['DESK']['oauth_token_secret'])

#Grabing the desk cases for the NPR App
new = Desk(sitename, auth)
new.pull_save_cases(request_url="/api/v2/cases/search?q=custom_new_app:true&per_page=100&page=1", fname=npr_app_data)

#Grabing updated list of all labels used in Desk
labels = Desk(sitename, auth)
labels.pull_save_cases(request_url="/api/v2/labels", fname=labels_data, fields=['name'])


# In[6]:


##########Change for Dataset#########
projectId = "midd-194719"
datasetId = "hippo"

tableIdNPRApp = "deskCasesNPRApp"
tableIdLabels = "deskLabels"
#####################################

#Deleteing and recreating the bq tables
del_recreate_bq(projectId, datasetId, tableIdNPRApp)
del_recreate_bq(projectId, datasetId, tableIdLabels)

#Has to Be imported beforehand
from google.cloud import bigquery

#Loading data from the NPR App and Labels to their tables
load_data_from_file(datasetId, tableIdNPRApp, npr_app_data)
load_data_from_file(datasetId, tableIdLabels, labels_data)
