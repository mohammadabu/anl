import requests
import psycopg2
import pprint
from datetime import datetime,date, timedelta

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

def getFollowersCountFB():
    followers_count = requests.get('https://graph.facebook.com/v9.0/17841400011449626/?fields=followers_count&access_token=EAAXUQ5082fwBAIsLwNPBr1xBvDVgZBvwHkcTZBBZBuSsow2lGu6keVP7PZA0kSPiR8oX7XjoWnDTlW8TCdR9auxJccKNVfhpSDheOATEdCZANRQyRGD6Xz8Gkl276cmKXuMPDgqH2TVSR5Uyo8W2YGqaZCIm3GCi8avlClzpv3JYEbn4qylsxg5aseWISUNJKIcODr4G2dfQZDZD')
    return followers_count.json()

def getInsightsMetricFB():
    insights_metric = requests.get('https://graph.facebook.com/v9.0/17841400011449626/insights?metric=impressions,reach,follower_count,email_contacts,phone_call_clicks,text_message_clicks,get_directions_clicks,website_clicks,profile_views&access_token=EAAXUQ5082fwBAIsLwNPBr1xBvDVgZBvwHkcTZBBZBuSsow2lGu6keVP7PZA0kSPiR8oX7XjoWnDTlW8TCdR9auxJccKNVfhpSDheOATEdCZANRQyRGD6Xz8Gkl276cmKXuMPDgqH2TVSR5Uyo8W2YGqaZCIm3GCi8avlClzpv3JYEbn4qylsxg5aseWISUNJKIcODr4G2dfQZDZD&date_preset=last_year&period=day')
    return insights_metric.json()

def getMostViewedPosts(entity_id,day):
    # most_viewed_posts = requests.get('https://graph.facebook.com/v9.0/103594884588/?fields=posts.since(2017-05-20).until(2017-05-21){id,created_time,insights.metric(post_impressions)}&since=1592772875&until=1593032140&access_token=EAAXUQ5082fwBAAc4Yv6r4okQw7VZAPpG6YxX7yZBItdpXZB8G9RfZAsOaegXzEjMXjh8DqIIYzcPgrMhJalAjAXxrq8ZB6DYWymHSmUKBRiC3Ib9SybeuACKnNY8Rf7jGf4gsPaAWjOSZAJRepKlNmsMmQAWjuZBqXI5k4C4TiZAS9mjZB1FcgihFcG9GXUp6aD7NkBWjYdZAHXQZDZD')
    entity_id = entity_id[2]
    token = entity_id[3]
    day_plus = day
    day_plus += timedelta(days=1)
    params = {
        'fields':f'posts.since({day}).until({day_plus})'+ '{id,created_time,insights.metric(post_impressions)}',
        'access_token':token
    }
    most_viewed_posts = requests.get(f'https://graph.facebook.com/v9.0/{entity_id}',params=params)
    return most_viewed_posts.json()

def saveMostViewedPosts(data):
    print(data)

def syncMostViewedPosts(entity_id,day):
    data = getMostViewedPosts(entity_id,day)
    saveMostViewedPosts(data)

def sync_facebook_account(entity_id,day):
    syncMostViewedPosts(entity_id,day)

def main():
    facebook_entity = get_facebook_entity()
    sdate = date(2020, 1, 1)
    edate = date(2021, 3, 29)  
    delta = edate - sdate
    for entity in facebook_entity:
        print('--------------------------------------')
        print(entity[4])
        for i in range(delta.days + 1):
            dayRange = sdate + timedelta(days=i)
            print('--------------------------------------')
            print(dayRange)
            sync_facebook_account(entity,dayRange)

def get_facebook_entity():
  cursor = connection.cursor()  
  pg_select = """ select * from consultancy_integrations."alex_fb_tokens" """ 
  cursor.execute(pg_select)
  record = cursor.fetchall()
  return record

if __name__ == '__main__':
    main()
