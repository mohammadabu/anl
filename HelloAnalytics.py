
import psycopg2
import time
import locale
import xlsxwriter
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

workbook_page_view = xlsxwriter.Workbook('page_view.xlsx')
worksheet_page_view = workbook_page_view.add_worksheet("page_view")

workbook_age_view = xlsxwriter.Workbook('age_view.xlsx')
worksheet_age_view = workbook_age_view.add_worksheet("age_view")

workbook_user_session_view = xlsxwriter.Workbook('user_session_view.xlsx')
worksheet_user_session_view = workbook_user_session_view.add_worksheet("user_session_view")

workbook_gender_view = xlsxwriter.Workbook('gender_view.xlsx')
worksheet_gender_view = workbook_gender_view.add_worksheet("gender_view")

workbook_device_view = xlsxwriter.Workbook('device_view.xlsx')
worksheet_device_view = workbook_device_view.add_worksheet("device_view")

workbook_default_channel_view = xlsxwriter.Workbook('default_channel_view.xlsx')
worksheet_default_channel_view = workbook_default_channel_view.add_worksheet("default_channel_view")

workbook_browser_view = xlsxwriter.Workbook('browser_view.xlsx')
worksheet_browser_view = workbook_browser_view.add_worksheet("browser_view")



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
  if data:
    cursor = connection.cursor()
    for x in data :
      worksheet_user_session_view.write(count_user_session, 0,VIEW_ID[0])
      worksheet_user_session_view.write(count_user_session, 1,VIEW_ID[1])
      worksheet_user_session_view.write(count_user_session, 2,VIEW_ID[2])
      worksheet_user_session_view.write(count_user_session, 3,VIEW_ID[3])
      worksheet_user_session_view.write(count_user_session, 4,VIEW_ID[4])
      worksheet_user_session_view.write(count_user_session, 5,data[x]['metric']['ga:users'])
      worksheet_user_session_view.write(count_user_session, 6,data[x]['metric']['ga:sessions'])
      worksheet_user_session_view.write(count_user_session, 7,data[x]['metric']['ga:organicSearches'])
      worksheet_user_session_view.write(count_user_session, 8,data[x]['metric']['ga:newUsers'])
      worksheet_user_session_view.write(count_user_session, 9,day_norway)
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
  if data:
    cursor = connection.cursor()
    for x in data:
      worksheet_gender_view.write(count_gender_session, 0,VIEW_ID[0])
      worksheet_gender_view.write(count_gender_session, 1,VIEW_ID[1])
      worksheet_gender_view.write(count_gender_session, 2,VIEW_ID[2])
      worksheet_gender_view.write(count_gender_session, 3,VIEW_ID[3])
      worksheet_gender_view.write(count_gender_session, 4,VIEW_ID[4])
      worksheet_gender_view.write(count_gender_session, 5,data[x]['metric']['ga:users'])
      worksheet_gender_view.write(count_gender_session, 6,data[x]['dimension']['ga:userGender'])
      worksheet_gender_view.write(count_gender_session, 7,day_norway)
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
  if data:
    cursor = connection.cursor()
    for x in data:
      worksheet_device_view.write(count_device_session, 0,VIEW_ID[0])
      worksheet_device_view.write(count_device_session, 1,VIEW_ID[1])
      worksheet_device_view.write(count_device_session, 2,VIEW_ID[2])
      worksheet_device_view.write(count_device_session, 3,VIEW_ID[3])
      worksheet_device_view.write(count_device_session, 4,VIEW_ID[4])
      worksheet_device_view.write(count_device_session, 5,data[x]['metric']['ga:users'])
      worksheet_device_view.write(count_device_session, 6,data[x]['dimension']['ga:deviceCategory'])
      worksheet_device_view.write(count_device_session, 7,data[x]['dimension']['ga:month'])
      worksheet_device_view.write(count_device_session, 8,day_norway)
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
  if data:
    cursor = connection.cursor()
    for x in data:
      worksheet_default_channel_view.write(count_default_channel_session, 0,VIEW_ID[0])
      worksheet_default_channel_view.write(count_default_channel_session, 1,VIEW_ID[1])
      worksheet_default_channel_view.write(count_default_channel_session, 2,VIEW_ID[2])
      worksheet_default_channel_view.write(count_default_channel_session, 3,VIEW_ID[3])
      worksheet_default_channel_view.write(count_default_channel_session, 4,VIEW_ID[4])
      worksheet_default_channel_view.write(count_default_channel_session, 5,data[x]['metric']['ga:users'])
      worksheet_default_channel_view.write(count_default_channel_session, 6,data[x]['metric']['ga:sessions'])
      worksheet_default_channel_view.write(count_default_channel_session, 7,data[x]['metric']['ga:organicSearches'])
      worksheet_default_channel_view.write(count_default_channel_session, 8,data[x]['dimension']['ga:month'])
      worksheet_default_channel_view.write(count_default_channel_session, 9,data[x]['dimension']['ga:channelGrouping'])
      worksheet_default_channel_view.write(count_default_channel_session, 10,day_norway)
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
  if data: 
    cursor = connection.cursor()
    for x in data:
      worksheet_browser_view.write(count_browser_session, 0,VIEW_ID[0])
      worksheet_browser_view.write(count_browser_session, 1,VIEW_ID[1])
      worksheet_browser_view.write(count_browser_session, 2,VIEW_ID[2])
      worksheet_browser_view.write(count_browser_session, 3,VIEW_ID[3])
      worksheet_browser_view.write(count_browser_session, 4,VIEW_ID[4])
      worksheet_browser_view.write(count_browser_session, 5,data[x]['metric']['ga:users'])
      worksheet_browser_view.write(count_browser_session, 6,data[x]['metric']['ga:sessions'])
      worksheet_browser_view.write(count_browser_session, 7,data[x]['dimension']['ga:browser'])
      worksheet_browser_view.write(count_browser_session, 8,data[x]['metric']['ga:organicSearches'])
      worksheet_browser_view.write(count_browser_session, 9,day_norway)
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
  if data:
    cursor = connection.cursor()
    for x in data:
      worksheet_age_view.write(count_age_report, 0,VIEW_ID[0])
      worksheet_age_view.write(count_age_report, 1,VIEW_ID[1])
      worksheet_age_view.write(count_age_report, 2,VIEW_ID[2])
      worksheet_age_view.write(count_age_report, 3,VIEW_ID[3])
      worksheet_age_view.write(count_age_report, 4,VIEW_ID[4])
      worksheet_age_view.write(count_age_report, 5,data[x]['metric']['ga:users'])
      worksheet_age_view.write(count_age_report, 6,data[x]['dimension']['ga:userAgeBracket'])
      worksheet_age_view.write(count_age_report, 7,day_norway)
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

def save_page_view_report(data,VIEW_ID,day):
  global count_page_view_report
  global insert_page_view_report_list
  day_norway =  convert_date(day) 
  if data :
    cursor = connection.cursor()
    for x in data:
      worksheet_page_view.write(count_page_view_report, 0,VIEW_ID[0])
      worksheet_page_view.write(count_page_view_report, 1,VIEW_ID[1])
      worksheet_page_view.write(count_page_view_report, 2,VIEW_ID[2])
      worksheet_page_view.write(count_page_view_report, 3,VIEW_ID[3])
      worksheet_page_view.write(count_page_view_report, 4,VIEW_ID[4])
      worksheet_page_view.write(count_page_view_report, 5,data[x]['metric']['ga:users'])
      worksheet_page_view.write(count_page_view_report, 6,data[x]['metric']['ga:pageviews'])
      worksheet_page_view.write(count_page_view_report, 7,data[x]['dimension']['ga:pagePath'])
      worksheet_page_view.write(count_page_view_report, 8,data[x]['dimension']['ga:month'])
      worksheet_page_view.write(count_page_view_report, 9,data[x]['metric']['ga:avgTimeOnPage'])
      worksheet_page_view.write(count_page_view_report, 10,day_norway)
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
  workbook_page_view.close()
  workbook_age_view.close()
  workbook_user_session_view.close()
  workbook_gender_view.close()
  workbook_device_view.close()
  workbook_default_channel_view.close()
  workbook_browser_view.close()
      
def get_google_entity():
  cursor = connection.cursor()
  # server 1
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 10 OFFSET 1 """
  # server 2
  pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 3 OFFSET 51 """ 
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
  # pg_select = """ select * from consultancy_integrations."alex_ga_tracking_id" limit 1 OFFSET 78 """ 
  cursor.execute(pg_select)
  record = cursor.fetchall()
  return record

if __name__ == '__main__':
  main()
