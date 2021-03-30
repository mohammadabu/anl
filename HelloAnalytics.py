
import psycopg2
import time
import locale
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime,date, timedelta

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

count_page_view_report = 0
insert_page_view_report_list = list()

count_age_report = 0
insert_age_report_list = list()

count_user_session = 0
insert_user_session_list = list()

count_gender_session = 0
insert_gender_session_list = list()

count_device_session = 0
insert_device_session_list = list()

count_default_channel_session = 0
insert_default_channel_list = list()

count_browser_session = 0
insert_browser_list = list()


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
def initialize_analyticsreporting(KEY_FILE_LOCATION):
  credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
  analytics = build('analyticsreporting', 'v4', credentials=credentials)
  return analytics

def print_response(response):
  new_response = {}
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    count = 0
    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])
      new_response[count] = {}
      new_response[count]['dimension'] = {}
      for header, dimension in zip(dimensionHeaders, dimensions):
        new_response[count]['dimension'][header] = dimension
        # print(header + ': ', dimension)

      for i, values in enumerate(dateRangeValues):
        # print('Date range:', str(i))
        new_response[count]['metric'] = {}
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          new_response[count]['metric'][metricHeader.get('name')] = value
          # print(metricHeader.get('name') + ':', value)
      count = count + 1    
    return  new_response     

def convert_date(day):
  locales = ['no_NO']
  for loc in locales:
    locale.setlocale(locale.LC_ALL, loc)
    day_norway = day.strftime("%A %d. %B %Y")

  return  day_norway

def save_user_session_report(data,VIEW_ID,day):
  global count_user_session
  global insert_user_session_list
  day_norway =  convert_date(day) 
  if(data):
    for x in data:
      cursor = connection.cursor()
      pg_insert = """ INSERT INTO consultancy_integrations."ga_users_sessions" (top_level, sub_folder, entity_id,community_id,community_name,users,sessions,organicsearches,newusers,maxusers,date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
      # inserted_values = (VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['metric']['ga:sessions'],data[x]['metric']['ga:organicSearches'],data[x]['metric']['ga:newUsers'],data[x]['metric']['ga:newUsers'],day_norway)
      # cursor.execute(pg_insert, inserted_values)
      # connection.commit()
      if count_user_session == 10000 or day == "2021-03-29": 
          cursor.executemany(pg_insert, insert_user_session_list)
          connection.commit()
          insert_user_session_list = list()
          count_page_view_report = 0
          print('save_user_session_report')
      else:
          insert_user_session_list.append([VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['metric']['ga:sessions'],data[x]['metric']['ga:organicSearches'],data[x]['metric']['ga:newUsers'],data[x]['metric']['ga:newUsers'],day_norway])  
      count_user_session += 1

def get_user_session_report(analytics,VIEW_ID,day):
  
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': str(day), 'endDate': str(day)}],
          'metrics': [{'expression':'ga:newUsers' },{'expression':'ga:users'},{'expression':'ga:organicSearches'},{'expression':'ga:sessions'}],
          # 'dimensions': [{'name':'ga:sessionCount'}],
        }]
      }
  ).execute()

def sync_user_session_report(analytics,VIEW_ID,day):
  data = get_user_session_report(analytics,VIEW_ID[2],day)
  data = print_response(data)
  save_user_session_report(data,VIEW_ID,day)

def save_gender_report(data,VIEW_ID,day):
  global count_gender_session
  global insert_gender_session_list
  day_norway =  convert_date(day) 
  if(data):
    for x in data:
      cursor = connection.cursor()
      pg_insert = """ INSERT INTO consultancy_integrations."ga_gender" (top_level, sub_folder, entity_id,community_id,community_name,users,gender,date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
      # inserted_values = (VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['dimension']['ga:userGender'],day_norway)
      # cursor.execute(pg_insert, inserted_values)
      # connection.commit()
      if count_gender_session == 10000 or day == "2021-03-29": 
          cursor.executemany(pg_insert, insert_gender_session_list)
          connection.commit()
          insert_gender_session_list = list()
          count_page_view_report = 0
          print('save_gender_report')
      else:
          insert_gender_session_list.append([VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['dimension']['ga:userGender'],day_norway])  
      count_gender_session += 1

def get_gender_report(analytics,VIEW_ID,day):
  
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': str(day), 'endDate': str(day)}],
          'metrics': [{'expression':'ga:users'}],
          'dimensions': [{'name':'ga:userGender'}],
        }]
      }
  ).execute()

def sync_gender_report(analytics,VIEW_ID,day):
  data = get_gender_report(analytics,VIEW_ID[2],day)
  data = print_response(data)
  save_gender_report(data,VIEW_ID,day)

def save_device_report(data,VIEW_ID,day):
  global count_device_session
  global insert_device_session_list
  day_norway =  convert_date(day) 
  if(data):
    for x in data:
      cursor = connection.cursor()
      pg_insert = """ INSERT INTO consultancy_integrations."ga_device" (top_level, sub_folder, entity_id,community_id,community_name,users,mobile_device_category,month_of_year,date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
      # inserted_values = (VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['dimension']['ga:deviceCategory'],data[x]['dimension']['ga:month'],day_norway)
      # cursor.execute(pg_insert, inserted_values)
      # connection.commit()
      if count_device_session == 10000 or day == "2021-03-29": 
          cursor.executemany(pg_insert, insert_device_session_list)
          connection.commit()
          insert_device_session_list = list()
          count_page_view_report = 0
          print('save_device_report')
      else:
          insert_device_session_list.append([VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['dimension']['ga:deviceCategory'],data[x]['dimension']['ga:month'],day_norway])  
      count_device_session += 1

def get_device_report(analytics,VIEW_ID,day):
  
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': str(day), 'endDate': str(day)}],
          'metrics': [{'expression':'ga:users'}],
          'dimensions': [{'name':'ga:deviceCategory'},{'name':'ga:month'}],
        }]
      }
  ).execute()

def sync_device_report(analytics,VIEW_ID,day):
  data = get_device_report(analytics,VIEW_ID[2],day)
  data = print_response(data)
  save_device_report(data,VIEW_ID,day)

def save_default_channel_report(data,VIEW_ID,day):
  global count_default_channel_session
  global insert_default_channel_list
  day_norway =  convert_date(day) 
  if(data):
    for x in data:
      cursor = connection.cursor()
      pg_insert = """ INSERT INTO consultancy_integrations."ga_default_channel" (top_level, sub_folder, entity_id,community_id,community_name,users,sessions,organicsearches,monthofyear,defaultchannelgrouping,date)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
      # inserted_values = (VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['metric']['ga:sessions'],data[x]['metric']['ga:organicSearches'],data[x]['dimension']['ga:month'],data[x]['metric']['ga:organicSearches'],day_norway)
      # cursor.execute(pg_insert, inserted_values)
      # connection.commit()

      if count_default_channel_session == 10000 or day == "2021-03-29": 
          cursor.executemany(pg_insert, insert_default_channel_list)
          connection.commit()
          insert_default_channel_list = list()
          count_page_view_report = 0
          print('save_default_channel_report')
      else:
          insert_default_channel_list.append([VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['metric']['ga:sessions'],data[x]['metric']['ga:organicSearches'],data[x]['dimension']['ga:month'],data[x]['metric']['ga:organicSearches'],day_norway])  
      count_default_channel_session += 1

def get_default_channel_report(analytics,VIEW_ID,day):
  
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': str(day), 'endDate': str(day)}],
          'metrics': [{'expression':'ga:organicSearches'},{'expression':'ga:users'},{'expression':'ga:sessions'}],
          'dimensions': [{'name':'ga:channelGrouping'},{'name':'ga:month'}],
        }]
      }
  ).execute()

def sync_default_channel_report(analytics,VIEW_ID,day):
  data = get_default_channel_report(analytics,VIEW_ID[2],day)
  data = print_response(data)
  save_default_channel_report(data,VIEW_ID,day)

def save_browser_report(data,VIEW_ID,day):
  global count_browser_session
  global insert_browser_list
  day_norway =  convert_date(day) 
  if(data):
    for x in data:
      cursor = connection.cursor()
      pg_insert = """ INSERT INTO consultancy_integrations."ga_browser" (top_level, sub_folder, entity_id,community_id,community_name,users,sessions,organicsearches,browser,date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
      # inserted_values = (VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['metric']['ga:sessions'],data[x]['dimension']['ga:browser'],data[x]['metric']['ga:organicSearches'],day_norway)
      # cursor.execute(pg_insert, inserted_values)
      # connection.commit()

      if count_browser_session == 10000 or day == "2021-03-29": 
          cursor.executemany(pg_insert, insert_browser_list)
          connection.commit()
          insert_browser_list = list()
          count_page_view_report = 0
          print('save_browser_report')
      else:
          insert_browser_list.append([VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['metric']['ga:sessions'],data[x]['dimension']['ga:browser'],data[x]['metric']['ga:organicSearches'],day_norway])  
      count_browser_session += 1

def get_browser_report(analytics,VIEW_ID,day):
  
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': str(day), 'endDate': str(day)}],
          'metrics': [{'expression':'ga:organicSearches'},{'expression':'ga:users'},{'expression':'ga:sessions'}],
          'dimensions': [{'name':'ga:browser'}],
        }]
      }
  ).execute()

def sync_browser_report(analytics,VIEW_ID,day):
  data = get_browser_report(analytics,VIEW_ID[2],day)
  data = print_response(data)
  save_browser_report(data,VIEW_ID,day)

def save_age_report(data,VIEW_ID,day):
  global count_age_report
  global insert_age_report_list
  day_norway =  convert_date(day) 
  if(data):
    cursor = connection.cursor()
    for x in data:
      pg_insert = """ INSERT INTO consultancy_integrations."ga_age" (top_level, sub_folder, entity_id,community_id,community_name,users,age,date)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

      if count_age_report == 10000 or day == "2021-03-29": 
          cursor.executemany(pg_insert, insert_age_report_list)
          connection.commit()
          insert_age_report_list = list()
          count_page_view_report = 0
          print('save_age_report')
      else:
          insert_age_report_list.append([VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['dimension']['ga:userAgeBracket'],day_norway])  
      count_age_report += 1

def get_age_report(analytics,VIEW_ID,day):
  
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': str(day), 'endDate': str(day)}],
          'metrics': [{'expression':'ga:users'}],
          'dimensions': [{'name':'ga:userAgeBracket'}],
        }]
      }
  ).execute()

def sync_age_report(analytics,VIEW_ID,day):
  data = get_age_report(analytics,VIEW_ID[2],day)
  data = print_response(data)
  save_age_report(data,VIEW_ID,day)

def save_user_report(data):
  cursor = connection.cursor()
  #Get the column name of a table inside the database and put some values
  pg_insert = """ INSERT INTO public."google-stats" (id, pageview, bounces, users, newusers,goal1starts,goal1completions)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""" 
  inserted_values = (data[''], 333, 333, 333, 333,333,333)
  cursor.execute(pg_insert, inserted_values)
  #Commit transaction and prints the result successfully
  connection.commit()

def get_user_report(analytics,VIEW_ID,day):
  
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': str(day), 'endDate': str(day)}],
          'metrics': [{'expression':'ga:users','expression':'ga:newUsers','expression':'ga:percentNewSessions'}],
          'dimensions': [{'name':'ga:userType','name':'ga:sessionCount'}],
        }]
      }
  ).execute()

def sync_user_report(analytics,VIEW_ID,day):
  data = get_user_report(analytics,VIEW_ID,day)
  data = print_response(data)
  print(data)
  # save_user_report(data)

def save_page_view_report(data,VIEW_ID,day):
  global count_page_view_report
  global insert_page_view_report_list
  day_norway =  convert_date(day) 
  if(data):
    cursor = connection.cursor()
    for x in data:
      pg_insert = """ INSERT INTO consultancy_integrations."ga_page_view" (top_level, sub_folder, entity_id,community_id,community_name,users,pageviews,page_url,month_of_year,avg_time_on_page,date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
      if count_page_view_report == 10000 or day == "2021-03-29": 
          cursor.executemany(pg_insert, insert_page_view_report_list)
          connection.commit()
          insert_page_view_report_list = list()
          count_page_view_report = 0
          print('save_page_view_report')
      else:
          insert_page_view_report_list.append([VIEW_ID[0], VIEW_ID[1], VIEW_ID[2], VIEW_ID[3], VIEW_ID[4],data[x]['metric']['ga:users'],data[x]['metric']['ga:pageviews'],data[x]['dimension']['ga:pagePath'],data[x]['dimension']['ga:month'],data[x]['metric']['ga:avgTimeOnPage'],day_norway])  

      count_page_view_report += 1
def get_page_view_report(analytics,VIEW_ID,day):
  
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': str(day), 'endDate': str(day)}],
          'metrics': [{'expression':'ga:users'},{'expression':'ga:pageviews'},{'expression':'ga:avgTimeOnPage'}],
          'dimensions': [{'name':'ga:pagePath'},{'name':'ga:month'}],
        }]
      }
  ).execute()

def sync_page_view_report(analytics,VIEW_ID,day):
  data = get_page_view_report(analytics,VIEW_ID[2],day)
  data = print_response(data)
  save_page_view_report(data,VIEW_ID,day)

def sync_google_account(analytics,VIEW_ID,day):
    sync_age_report(analytics,VIEW_ID,day)
    sync_browser_report(analytics,VIEW_ID,day)
    sync_default_channel_report(analytics,VIEW_ID,day)
    sync_gender_report(analytics,VIEW_ID,day)
    sync_user_session_report(analytics,VIEW_ID,day)
    sync_device_report(analytics,VIEW_ID,day)
    sync_page_view_report(analytics,VIEW_ID,day)

def main():
  google_entity = get_google_entity()
  sdate = date(2020, 1, 1)
  edate = date(2021, 3, 29)  
  delta = edate - sdate
  for entity in google_entity:
    print('--------------------------------------')
    print(entity[4])
    for i in range(delta.days + 1):
      dayRange = sdate + timedelta(days=i)
      print('--------------------------------------')
      print(dayRange)
      analytics = initialize_analyticsreporting('./key1.json')
      sync_google_account(analytics,entity,dayRange)
      
def get_google_entity():
  cursor = connection.cursor()
  # server 1
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 0 """
  # server 2
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 6 """ 
  # server 3
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 9 """
  # server 4
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 12 """
  # server 5
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 15 """ 
  # server 6
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 18 """ 
  # server 7
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 18 """ 
  # server 8
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 21 """ 
  # server 9
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 24 """ 
  # server 10
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 27 """ 
  # server 11
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 30 """ 
  # server 12
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 33 """ 
  # server 13
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 36 """ 
  # server 14
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 39 """ 
  # server 15
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 42 """ 
  # server 16
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 45 """ 
  # server 17
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 48 """ 
  # server 18
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 51 """ 
  # server 19
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 54 """ 
  # server 20
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 57 """ 
  # server 21
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 60 """ 
  # server 22
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 63 """ 
  # server 23
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 66 """ 
  # server 24
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 69 """ 
  # server 25
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 72 """ 
  # server 26
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 75 """ 
  # server 27
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 78 """ 
  cursor.execute(pg_select)
  record = cursor.fetchall()
  return record

if __name__ == '__main__':
  main()
  # cursor = connection.cursor()
  # pg_insert = """ INSERT INTO consultancy_integrations."test_cons_int" (test_1, test_2)
  #               VALUES (%s,%s)"""
  # insert_vendor_list = list()
  # insert_vendor_list.append(["1","2"])
  # insert_vendor_list.append(["1","2"])
  # insert_vendor_list.append(["1","2"])
  # cursor.executemany(pg_insert, insert_vendor_list)
  # connection.commit()
  # print(insert_vendor_list)
      