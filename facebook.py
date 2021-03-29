    # "https://graph.facebook.com/v9.0/103594884588/insights?metric=page_posts_impressions,page_impressions&access_token=EAAXUQ5082fwBAAc4Yv6r4okQw7VZAPpG6YxX7yZBItdpXZB8G9RfZAsOaegXzEjMXjh8DqIIYzcPgrMhJalAjAXxrq8ZB6DYWymHSmUKBRiC3Ib9SybeuACKnNY8Rf7jGf4gsPaAWjOSZAJRepKlNmsMmQAWjuZBqXI5k4C4TiZAS9mjZB1FcgihFcG9GXUp6aD7NkBWjYdZAHXQZDZD&since=1617042121&until=1617045121"
    # "https://graph.facebook.com/v9.0/103594884588/insights?metric=page_fans&access_token=EAAXUQ5082fwBAAc4Yv6r4okQw7VZAPpG6YxX7yZBItdpXZB8G9RfZAsOaegXzEjMXjh8DqIIYzcPgrMhJalAjAXxrq8ZB6DYWymHSmUKBRiC3Ib9SybeuACKnNY8Rf7jGf4gsPaAWjOSZAJRepKlNmsMmQAWjuZBqXI5k4C4TiZAS9mjZB1FcgihFcG9GXUp6aD7NkBWjYdZAHXQZDZD&since=1615042121&until=1617045121"
    # most_viewed_posts = requests.get('https://graph.facebook.com/v9.0/103594884588/?fields=posts.since(2017-05-20).until(2017-05-21){id,created_time,insights.metric(post_impressions)}&since=1592772875&until=1593032140&access_token=EAAXUQ5082fwBAAc4Yv6r4okQw7VZAPpG6YxX7yZBItdpXZB8G9RfZAsOaegXzEjMXjh8DqIIYzcPgrMhJalAjAXxrq8ZB6DYWymHSmUKBRiC3Ib9SybeuACKnNY8Rf7jGf4gsPaAWjOSZAJRepKlNmsMmQAWjuZBqXI5k4C4TiZAS9mjZB1FcgihFcG9GXUp6aD7NkBWjYdZAHXQZDZD')

import requests
import psycopg2
import pprint
import locale
import time
from datetime import datetime,date, timedelta
# from datetime import datetime, time ,timedelta

try:
  connection = psycopg2.connect(
      user = "y_ayyoub",
      password = "zer3raiw7YUMPknup",
      host = "reporting.chvcwsrzy2fr.eu-west-1.redshift.amazonaws.com",
      port = "5439",
      database = "reportingprod"
  )
  cursor = connection.cursor()
  print(connection.get_dsn_parameters(),"\n")
  cursor.execute("SELECT version();")
  record = cursor.fetchone()
except(Exception, psycopg2.Error) as error:
  print("Error connecting to PostgreSQL database", error)
  connection = None


def convert_date(day):
  locales = ['us_US']
  for loc in locales:
    locale.setlocale(locale.LC_ALL, loc)
    day_norway = day.strftime("%A %d. %B %Y")

  return  day_norway

def getFBEngagement(entity_id,day):
    entity = entity_id[2]
    token = entity_id[3]
    day_plus = day
    day_plus += timedelta(days=1)

    day_stamp = time.mktime(day.timetuple())
    day_plus_stamp = time.mktime(day_plus.timetuple())

    params = {
        'metric':'page_post_engagements',
        'access_token':token,
        'since':day_stamp,
        'until':day_plus_stamp
    }
    dailyReach = requests.get(f'https://graph.facebook.com/v9.0/{entity}/insights',params=params)
    return dailyReach.json()

def saveFBEngagement(data,entity_id,day):
    account_id = entity_id[2]
    community_id = entity_id[1]
    community_name = entity_id[0]
    day = day.strftime("%m.%d.%Y") 
    if "data" in data:
        for followers in data['data']:
            for val in followers['values']:
                cursor = connection.cursor()
                pg_insert = """ INSERT INTO consultancy_integrations."fbengagement" (community_id, community_name, account_id,name,period,fb_value,fb_date)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)"""
                inserted_values = (community_id, community_name, account_id,followers['name'],followers['period'],val['value'],day)
                cursor.execute(pg_insert, inserted_values)
                connection.commit()       

def syncFBEngagement(entity_id,day):
    data = getFBEngagement(entity_id,day)
    saveFBEngagement(data,entity_id,day)

def getFBFollowers(entity_id,day):
    entity = entity_id[2]
    token = entity_id[3]
    day_plus = day
    day_plus += timedelta(days=1)

    day_stamp = time.mktime(day.timetuple())
    day_plus_stamp = time.mktime(day_plus.timetuple())

    params = {
        'metric':'page_fans',
        'access_token':token,
        'since':day_stamp,
        'until':day_plus_stamp
    }
    dailyReach = requests.get(f'https://graph.facebook.com/v9.0/{entity}/insights',params=params)
    return dailyReach.json()

def saveFBFollowers(data,entity_id,day):
    account_id = entity_id[2]
    community_id = entity_id[1]
    community_name = entity_id[0]
    day = day.strftime("%m.%d.%Y")
    if "data" in data: 
        for followers in data['data']:
            for val in followers['values']:
                cursor = connection.cursor()
                pg_insert = """ INSERT INTO consultancy_integrations."fbfollowers" (community_id, community_name, account_id,name,period,fbfollowersval,fb_date)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)"""
                inserted_values = (community_id, community_name, account_id,followers['name'],followers['period'],val['value'],day)
                cursor.execute(pg_insert, inserted_values)
                connection.commit()    

def syncFBFollowers(entity_id,day):
    data = getFBFollowers(entity_id,day)
    saveFBFollowers(data,entity_id,day)           

def getFBDailyReach(entity_id,day):
    entity = entity_id[2]
    token = entity_id[3]
    day_plus = day
    day_plus += timedelta(days=1)

    day_stamp = time.mktime(day.timetuple())
    day_plus_stamp = time.mktime(day_plus.timetuple())

    params = {
        'metric':'page_posts_impressions,page_impressions',
        'access_token':token,
        'since':day_stamp,
        'until':day_plus_stamp
    }
    dailyReach = requests.get(f'https://graph.facebook.com/v9.0/{entity}/insights',params=params)
    return dailyReach.json()

def saveFBDailyReach(data,entity_id,day):
    account_id = entity_id[2]
    community_id = entity_id[1]
    community_name = entity_id[0]
    day = day.strftime("%m.%d.%Y") 
    if "data" in data:
        for reach in data['data']:
            for val in reach['values']:
                cursor = connection.cursor()
                pg_insert = """ INSERT INTO consultancy_integrations."fbdailydata" (community_id, community_name, account_id,name,period,fb_value,fb_date)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)"""
                inserted_values = (community_id, community_name, account_id,reach['name'],reach['period'],val['value'],day)
                cursor.execute(pg_insert, inserted_values)
                connection.commit()

def syncFBDailyReach(entity_id,day):
    data = getFBDailyReach(entity_id,day)
    saveFBDailyReach(data,entity_id,day)

def getMostViewedPosts(entity_id,day):
    entity = entity_id[2]
    token = entity_id[3]
    day_plus = day
    day_plus += timedelta(days=1)
    params = {
        'fields':f'posts.since({day}).until({day_plus})'+ '{id,created_time,insights.metric(post_impressions)}',
        'access_token':token,
        'period':'day'
    }
    most_viewed_posts = requests.get(f'https://graph.facebook.com/v9.0/{entity}',params=params)
    return most_viewed_posts.json()

def saveMostViewedPosts(data,entity_id,day):
    account_id = entity_id[2]
    community_id = entity_id[1]
    community_name = entity_id[0]
    day = day.strftime("%m.%d.%Y") 
    if "posts" in data:
        for posts in data['posts']['data']:
            for insights in posts['insights']['data']:
                for val in insights['values']:
                    cursor = connection.cursor()
                    pg_insert = """ INSERT INTO consultancy_integrations."fbmostviewedposts" (community_id, community_name, account_id,name,period,fb_value,fb_date)
                                VALUES (%s,%s,%s,%s,%s,%s,%s)"""
                    inserted_values = (community_id, community_name, account_id,insights['name'],insights['period'],val['value'],day)
                    cursor.execute(pg_insert, inserted_values)
                    connection.commit()

def syncMostViewedPosts(entity_id,day):
    data = getMostViewedPosts(entity_id,day)
    saveMostViewedPosts(data,entity_id,day)

def sync_facebook_account(entity_id,day):
    syncMostViewedPosts(entity_id,day)
    syncFBDailyReach(entity_id,day)
    syncFBFollowers(entity_id,day)
    syncFBEngagement(entity_id,day)

def main():
    facebook_entity = get_facebook_entity()
    sdate = date(2020, 1, 1)
    edate = date(2021, 3, 29)  
    delta = edate - sdate
    for entity in facebook_entity:
        print('--------------------------------------')
        print(entity[4])
        print(entity[0])
        for i in range(delta.days + 1):
            dayRange = sdate + timedelta(days=i)
            print('--------------------------------------')
            print(dayRange)
            sync_facebook_account(entity,dayRange)

def get_facebook_entity():
  cursor = connection.cursor()  
  #tab 1   
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 0  """ 
  #tab 2
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 2  """ 
  #tab 3
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 4 """ 
  #tab 4
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 6  """ 
  #tab 5
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 8  """ 
  #tab 6
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 10 """ 
  #tab 7
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 12 """ 
  #tab 8
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 14  """ 
  #tab 9
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 16 """ 
  #tab 10
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 18 """ 
  #tab 11
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 20 """ 
  #tab 12
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 22  """ 
  #tab 13
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 24 """ 
  #tab 14
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 26 """ 
  #tab 15
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 28  """ 
  #tab 16
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 30 """
  #tab 17
  #pg_select = """ select * from consultancy_integrations."alex_fb_tokens" limit 2 OFFSET 32 """
  cursor.execute(pg_select)
  record = cursor.fetchall()
  return record

if __name__ == '__main__':
    main()
