#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  3 13:22:58 2018

Script that populates a local MongoDB with data from postgres as well as some 
user defined variables.

Use this to create a MongoDb Collection to create data for campaign analytics

TODO:
    - Campaign should work with more than one keywords...

@author: teemu
"""

#%%
import pandas as pd
import numpy as np
import psycopg2
import datetime as dt
import urllib3
from pymongo import MongoClient
import os
from dotenv import load_dotenv, find_dotenv

def create_campaign_data ():
    df_campaigns = pd.DataFrame({
        'campaign_id': [
            'campaign_boat',
            'campaign_tables',
            'campaign_september'
        ],
        'campaign_type': [
            'banner',
            'banner',
            'banner'
        ],
        'segment_keyword': [
            'boat',
            'tables',
            'september'
        ],
        'campaign_location': [
            'https://www.essentialibiza.com/',
            'https://www.essentialibiza.com/',
            'https://www.essentialibiza.com/ibiza-party-calendar/'
        ],
        'campaign_destination': [
            'https://www.essentialibiza.com/ibiza-boat-party/',
            'https://www.essentialibiza.com/ibiza-club-tables/',
            'https://www.essentialibiza.com/ibiza-party-calendar/september/'
        ],
        'campaign_starttime': [
            dt.datetime(2018, 8, 22, 6, 30),
            dt.datetime(2018, 8, 25),
            dt.datetime(2018, 9, 3, 18, 45)
        ],
        'campaign_endtime': [
            dt.datetime(2099,1,1),
            dt.datetime(2099,1,1),
            dt.datetime(2099,1,1)
        ]
    })
    return df_campaigns

def connect_to_postgres ():
    # find .env automagically by walking up directories until it's found
    dotenv_path = find_dotenv()
    # load up the entries as environment variables
    load_dotenv(dotenv_path)
    
    db_vedra = os.environ.get('DATABASE')
    user_vedra = os.environ.get('DB_USER')
    host_vedra = os.environ.get('DB_HOST')
    password_vedra = os.environ.get('DB_PASSWORD')
    db_vedra_connection_string = ("dbname=%s user=%s host=%s password=%s" 
                              % (db_vedra,
                                 user_vedra,
                                 host_vedra,
                                 password_vedra))

    try:
        conn = psycopg2.connect(db_vedra_connection_string)
    except Exception as err:
        print (err)
        print ('Unable to connect to postgres')
        
    return conn
        
def create_collection_to_mongo (collection_name, db_name='vedra'):
    try:
        client = MongoClient()
        client.server_info()
    except Exception as err:
        print (err)
        print ('Unable to connect to MongoDB')
    db = client[db_name]
    collection_name = db.collection_name
    return collection_name


def populate_mongo_with_postgres_data (campaigns, start, end):

    for i in range((len(campaigns))):
        # Query data and create a dataframe
        cur = conn.cursor()
        pg_select = (
            """
            with browsers as(
            	select distinct
            	    s.browser_id
            	from
            	    tbl_actions as a
            	    inner join
            	    tbl_sessions as s
            	    on a.session_id = s.session_id
            	where
            	    a.time between %d and %d
            	    and (lower(a.location) like lower('%s')
            	    or lower(a.page_title) like lower('%s'))
            	group by
            	    s.browser_id
            )
            select distinct
            	a.time,
                a.location,
                a.referrer,
                a.page_title,
                a.user_agent,
                a.remote_ip,
                a.session_id,
                a.language,
                s.browser_id,
                s.channel_id,
                f.do_not_track,
                f.ad_block,
                f.timezone_offset,
                f.cookies
            from tbl_actions as a 
            inner join tbl_sessions as s
                on a.session_id = s.session_id
            inner join tbl_fingerprints as f
                on s.fingerprint_hash = f.fingerprint_hash
            inner join browsers as b
            	on b.browser_id = s.browser_id
            where a.time between %d and %d;
            """ % (start,
                   end,
                   '%' + campaigns.loc[i]['segment_keyword'] + '%',
                   '%' + campaigns.loc[i]['segment_keyword'] + '%',
                   start,
                   end))
        cur.execute(pg_select)
        names = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        df = pd.DataFrame.from_records(data, columns=names)
        
        # Data manipulations
        def find_after(string, keyword):
            if string.find(keyword) == -1:
                return ''
            else:
                return string[string.find(keyword) + len(keyword):]
        
        def remove_after(string, keyword):
            if string.find(keyword) == -1:
                return string
            else:
                return string[:string.find(keyword)]
            
        df['path'] = df['location'].apply(lambda x: urllib3.util.parse_url(x)[4])
        df['search'] = df['location'].apply(lambda x: find_after(x, '?s='))
        df['location'] = df['location'].apply(lambda x: remove_after(x, '?s='))
        
        conditions = [(df['channel_id']=='4dd391b3792354db97c049f3c0c066a6') | 
                      (df['channel_id']=='11111111'),
                      (df['channel_id']=='10000016')]
        choices_channel = ['4dd391b3792354db97c049f3c0c066a6',
                           '10000016']
        choices_url = ['https://www.essentialibiza.com/',
                       'https://www.internationalmusicsummit.com/']
        df['channel'] = np.select(conditions, choices_channel, default='None')
        df['base_url'] = np.select(conditions, choices_url, default='None')
        
        df['time'] = pd.to_datetime(df['time'], unit='s')
        
        # Create list of dicts
        json_list = []
        for row in df.itertuples():
            json = {
                    'anonymous_id': getattr(row, 'browser_id'),
                    'campaign': {
                        'campaign_id': campaigns.loc[i]['campaign_id'],
                        'campaign_type': campaigns.loc[i]['campaign_type'],
                        'campaign_location': campaigns.loc[i]['campaign_location'],
                        'campaign_destination': campaigns.loc[i]['campaign_destination'],
                        'campaign_starttime': campaigns.loc[i]['campaign_starttime'],
                        'campaign_endtime': campaigns.loc[i]['campaign_endtime']
                    },
                    'timestamps':{
                        'initial_timestamp': getattr(row, 'time'),
                        'sent_timestamp': getattr(row, 'time'),
                        'received_timestamp': getattr(row, 'time'),
                        'timestamp': getattr(row, 'time')
                    },
                    'channel':{
                        'channel_id':  getattr(row, 'channel'),
                        'base_url': getattr(row, 'base_url')
                    },
                    'ip': getattr(row, 'remote_ip'),
                    'browser':{
                        'user_agent': getattr(row, 'user_agent'),
                        'locale': getattr(row, 'language'),
                        'cookies': getattr(row, 'cookies'),
                        'do_not_track': getattr(row, 'do_not_track'),
                        'ad_block': getattr(row, 'ad_block'),
                        'timezone': getattr(row, 'timezone_offset'),
                        'path': getattr(row, 'path'),
                        'referrer': getattr(row, 'referrer'),
                        'search': getattr(row, 'search'),
                        'title': getattr(row, 'page_title'),
                        'url': getattr(row, 'location')
                    }
                    }
            json_list.append(json)
        return json_list


conn = connect_to_postgres()

campaigns = create_collection_to_mongo(collection_name='campaigns')

df_campaigns = create_campaign_data()     

json_list = populate_mongo_with_postgres_data(campaigns = df_campaigns,
                                              start = dt.datetime(2018, 9, 9).timestamp(),
                                              end = dt.datetime(2018, 9, 10).timestamp())
campaigns_id = campaigns.insert_many(json_list)

# Create index(es)
campaigns.create_index('anonymous_id')

