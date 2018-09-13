#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  4 13:45:02 2018

Early schetch of campaign analytics ETL pipeline from MongoDB to JSON

TODO:

Tests for field requirements:
 
    Requested timeinterval should be so that:
        - Endtime cannot be $lt start of campaign
        - Starttime cannot be %gt end of campaign
    
    


@author: teemu
"""

#%%

from pymongo import MongoClient
import pandas as pd
import datetime as dt
import time

def connect_to_mongo():
    try:
        client = MongoClient()
        client.server_info()
    except Exception as err:
        print (err)
        print ('Unable to connect to MongoDB')
    return client


client = connect_to_mongo()
db = client['vedra']
campaigns = db.campaigns

def get_crawlers():
    crawlers = pd.read_csv('data/external/crawlers.csv', sep='\t', header=None)
    crawlers = crawlers[0].tolist()
    return crawlers



# unique_campaigns = campaigns.distinct('campaign.campaign_id')
#    starttime = campaigns.distinct('campaign.campaign_starttime',
#                                   {'campaign.campaign_id': campaign_id})
#    endtime = campaigns.distinct('campaign.campaign_endtime',
#                                 {'campaign.campaign_id': campaign_id})


def create_pipelines(startdate, enddate, campaign_id, channel, collection, session_time=(30 * 60 * 1000)):
    
    location = collection.distinct('campaign.campaign_location',
                                  {'campaign.campaign_id': campaign_id})
    destination = collection.distinct('campaign.campaign_destination',
                                     {'campaign.campaign_id': campaign_id})

    
    # General pipeline stages
        
    match = [ 
        {'$match': {
            '$and': [
                {'user_agent': {'$nin': crawlers}},
                {'timestamps.timestamp': {'$gte': startdate,
                                          '$lte': enddate}},
                {'channel.channel_id': {'$eq': channel}},
                {'campaign.campaign_id': campaign_id}
            ]
        }
        }
    ]
    
    convert_date = [
        {'$addFields': {
            'date': {
                '$dateFromParts':{
                        'year': {'$year': '$timestamps.timestamp'},
                        'month': {'$month': '$timestamps.timestamp'},
                        'day': {'$dayOfMonth': '$timestamps.timestamp'}
                        }
            }
        } 
        }
    ]
            
    sort = [{'$sort': {'_id': 1}}]

    # Pipeline for daily pageviews and unique users
    pipeline_browsers_pageviews = [
        {'$group': {
            '_id': {
                'date': '$date',
                'browser': '$anonymous_id'
            },
            'pageviews_browser_day': {'$sum': 1}
        }
        },
        {'$group': {'_id': '$_id.date',
                    'browsers': {'$sum': 1},
                    'page_views': {'$sum': '$pageviews_browser_day'}
                    }
        }
    ]
    
    # Pipeline for daily returning and firsttime users
    pipeline_returning_browsers = [
        {'$sort': {'anonymous_id': 1,
                   'timestamps.timestamp': 1}},
        # Create a list with all timestamps from a anonymous_id (browser)
        {'$group': {'_id': {
                        'browser': '$anonymous_id',
                        'date': '$date'}, 
                    'times': {'$push': '$timestamps.timestamp'}
                    }
        },
        # Add numeric index so we can use arrayElemAt
        {'$addFields':
            {'times_with_index':
                {'$zip':
                    {'inputs':
                        ['$times', {'$range': [0, {'$size': "$times"}]}]
                    }
                }
            }
        },
        # Get timestamp from previous record if applicaple
        # otherwise use records own timestamp 
        {
        '$project': {
            'date': 1,
            'time_pairs': {
                '$map': {
                    'input': '$times_with_index',
                    'in' : {
                        'current': {'$arrayElemAt': ['$$this', 0]},
                        'prev': {
                            '$arrayElemAt': [
                                '$times',
                                {'$max': [
                                    0,
                                    {'$subtract':
                                        [{'$arrayElemAt': ["$$this", 1]},1]}
                                    ]
                                }
                            ]
                        }
                    }
                }
            }
        }
        },
        {'$unwind': "$time_pairs"},
        {'$project': {
            'date': 1,
            'time_diff':  {
                '$subtract': ["$time_pairs.current", "$time_pairs.prev"]},
            'time': '$time_pairs.current'
            }
        },
        # users first sessions are now flagged if timediff between actions is 
        # equal to 0. This works under the assumption that users can't open two
        # pages at the exact same time
        {'$addFields': {
            'returning_session': {'$cond': [{'$gt': ['$time_diff', session_time]}, 1,  0]},
            'first_session': {'$cond': [{'$eq': ['$time_diff', 0]}, 1, 0]}
            }
        },
        {'$group': {
            '_id': '$_id.date',
            'returning_browsers': {'$sum': '$returning_session'},
            'first_timers': {'$sum': '$first_session'}
            }
        }
    ]
    
    # Pipeline for daily impressions
    pipeline_impressions = [
        {'$match': {'browser.url': {'$in': location}}
        },
        {'$group': {
           '_id': {
                'date': '$date',
                'browser': '$anonymous_id'
                },
            'impressions_per_browser': {'$sum': 1}
            }
        },
        {'$group': {
            '_id': '$_id.date',
            'unique_impressions': {'$sum': 1},
            'total_impressions': {'$sum': '$impressions_per_browser'}
            }
        },
    ]
    
    # Pipeline for daily interactions
    pipeline_interactions = [
        {'$match': {
            '$and': [
                {'browser.url': {'$in': destination}},
                {'browser.referrer': {'$in': location}}
                ]
         }
        },
        {'$group': {
             '_id': {
                 'date': '$date',
                 'browser': '$anonymous_id'
             },
             'interactions_per_browser': {'$sum': 1}
         }
        },
        {'$group': {
            '_id': '$_id.date',
            'unique_interactions': {'$sum': 1},
            'total_interactions': {'$sum': '$interactions_per_browser'}
            }
        }
    ]
    
    pipeline_browsers_pageviews = match + convert_date + pipeline_browsers_pageviews + sort
    pipeline_returning_browsers = match + pipeline_returning_browsers
    pipeline_impressions = match + convert_date + pipeline_impressions + sort
    pipeline_interactions = match + convert_date + pipeline_interactions + sort
    
    return (
        pipeline_browsers_pageviews,
        pipeline_returning_browsers,
        pipeline_impressions,
        pipeline_interactions
        )
 
#
crawlers = get_crawlers()
pipelines = create_pipelines(
    channel = '4dd391b3792354db97c049f3c0c066a6',
    startdate = dt.datetime(2018, 9, 4),
    enddate = dt.datetime(2018, 9, 11),
    campaign_id = 'campaign_boat',
    collection = campaigns
    )
#%%
start_pl1 = time.time()
pageviews_per_day = list(campaigns.aggregate(pipelines[0]))
end_pl1 = time.time()

start_pl2 = time.time()
returning_browsers_per_day = list(campaigns.aggregate(pipelines[1],
                                                      allowDiskUse=True)
        )
end_pl2 = time.time()

start_pl3 = time.time()
impressions_per_day = list(campaigns.aggregate(pipelines[2]))
end_pl3 = time.time()

start_pl4 = time.time()
interactions_per_day = list(campaigns.aggregate(pipelines[3]))
end_pl4 = time.time()
   
#%%

from functools import reduce
dfs = [pd.DataFrame.from_dict(pageviews_per_day),
       pd.DataFrame.from_dict(returning_browsers_per_day),
       pd.DataFrame.from_dict(impressions_per_day),
       pd.DataFrame.from_dict(interactions_per_day)
       ]
df_final = reduce(lambda left, right: pd.merge(left, right, on='_id'), dfs) 

#%%
print('execute time for pipeline 1: ' + str(end_pl1-start_pl1) + ' seconds')
print('execute time for pipeline 2: ' + str(end_pl2-start_pl2) + ' seconds')
print('execute time for pipeline 3: ' + str(end_pl3-start_pl3) + ' seconds')
print('execute time for pipeline 3: ' + str(end_pl4-start_pl4) + ' seconds')



