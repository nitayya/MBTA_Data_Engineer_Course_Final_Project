import requests
import pymongo
import telebot
from requests.structures import CaseInsensitiveDict
import datetime
from datetime import date
import configuration as c

#### Defining Constant Variables ####
bot = telebot.TeleBot(c.telegram_TOKEN)

#### Defining MongoDB Constant Variables ####
myclient = pymongo.MongoClient(c.mongo_client_url)
mydb = myclient[c.mongodb_client]
mycol = mydb[c.mongodb_db]

#### Generating Today's date ####
today = date.today()
current_date = today.strftime("%d_%m_%Y")

def format_time():
    t = datetime.datetime.now()
    s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
    return s[:-3]

#### Telegram Bot Message to User When prediction time Has Changed ####
def send_message_to_user(type_of_change,chat_id,user_first_name,origin_stop_name, dest_stop_name, predicted_arrival_time,new_prediction_time):
  if type_of_change=='early':
    msg = f'''*Hi {user_first_name}, Good news!*
    
The prediction time for your trip -
from {origin_stop_name} to {dest_stop_name}
was at {predicted_arrival_time},
*the new predicted arrival time is at {new_prediction_time}*
    '''
    bot.send_message(chat_id, msg, parse_mode= 'Markdown' )
  if type_of_change == 'late':
    msg = f'''*Hi {user_first_name}, Bad news!*
    
The prediction time for your trip -
from {origin_stop_name} to {dest_stop_name}
was at {predicted_arrival_time},
*the new predicted arrival time is at {new_prediction_time}*
        '''
    bot.send_message(chat_id, msg, parse_mode= 'Markdown' )

#### MBTA prediction time ######
def mbta_check_prediction_time(trip_id, origin_stop_id):
  try:
    from_url = f'https://api-v3.mbta.com/predictions?filter[trip]={trip_id}&filter[stop]={origin_stop_id}&filter=api-key={c.MBTA_API_KEY}'
    resp = requests.get(from_url)
    if resp.status_code != 200:
      return
    resp = resp.json()
    if (len(resp['data'])) == 0:
      return ''
    # get predicted arrival time - if its not None
    if resp["data"][0]["attributes"]["arrival_time"] is not None:
      predicted_arrival_time = resp["data"][0]["attributes"]["arrival_time"]
      predicted_arrival_time = predicted_arrival_time[:10] + " " + predicted_arrival_time[11:19]
    else:
      predicted_arrival_time = ''

    # get predicted departure time - if its not None
    if resp["data"][0]["attributes"]["departure_time"] is not None:
      predicted_departure_time = resp["data"][0]["attributes"]["departure_time"]
    else:
      predicted_departure_time = ''

    # if arrival time is None and departure time is not None, arrival time = departure time
    if predicted_arrival_time == '' and predicted_departure_time != '':
      predicted_arrival_time = predicted_departure_time

    return predicted_arrival_time
  except:
    return

#### Updating The Relevant Request In MongoDB As Not Active ####
def update_mongo_not_active(request_id):
  myquery = {"_id": request_id}
  newvalues = {"$set": {"is_active": 0}}
  mycol.update_one(myquery, newvalues)

#### Updating The New Request In MongoDB With The New Price ####
def insert_mongo_new_predicted_time(predicted_arrival_time , chat_id , user_id , origin_stop_id , destination_stop_id ,route_id,route_fare_class,route_name, trip_id , duration ,user_user_name
                           ,user_first_name, user_last_name , current_ts):
  mydict = {"predicted_arrival_time":predicted_arrival_time
    ,"chat_id":chat_id
    , "user_id":user_id
    , "origin_stop_id":origin_stop_id
    , "destination_stop_id":destination_stop_id
    , "trip_id":trip_id
    ,'origin_stop_name':origin_stop_name
    ,'dest_stop_name':dest_stop_name
    ,'route_id': route_id
    ,'route_fare_class':route_fare_class
    ,'route_name':route_name
    , "duration":duration
    , "user_user_name":user_user_name
    , "user_first_name":user_first_name
    , "user_last_name":user_last_name
    ,"current_ts":current_ts
    ,"is_active":1}
  record = mycol.insert_one(mydict)

#### Quering MongoDB Only With The Active Requests ####
myquery = { "is_active": 1 }
mongodb_client = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = mongodb_client[c.mongodb_client]
mycol = mydb[c.mongodb_db]

#### Pulling The Parameters Of The Active Requests In Order To Send To MBTA API ####
for record in mycol.find(myquery):
  request_id = record["_id"]
  predicted_arrival_time = record['predicted_arrival_time']
  chat_id = record['chat_id']
  user_id = record['user_id']
  #if record['origin_stop_id'] != None:
  origin_stop_id = record['origin_stop_id']
  #if record['destination_stop_id'] is not None:
  destination_stop_id = record['destination_stop_id']
  origin_stop_name = record['origin_stop_name']
  dest_stop_name = record['dest_stop_name']
  route_id = record['route_id'],
  route_fare_class = record['route_fare_class'],
  route_name = record['route_name'],
  trip_id = record['trip_id']
  duration = record['duration']
  #if record['user_user_name'] != None:
  user_user_name = record['user_user_name']
  #if record['user_first_name'] != None:
  user_first_name =  record['user_first_name']
  #if record['user_last_name'] != None:
  user_last_name = record['user_last_name']
  new_timestamp = format_time()

  #### Getting The New prediction Back From API ####
  new_prediction_time = mbta_check_prediction_time(trip_id, origin_stop_id)

  if new_prediction_time:
    # change the new date and time to string
    new_prediction_time_str = new_prediction_time[:10] + " " + new_prediction_time[11:19]
    new_prediction_time = datetime.datetime.strptime(new_prediction_time_str, "%Y-%m-%d %H:%M:%S")
    original_prediction_time = datetime.datetime.strptime(predicted_arrival_time, "%Y-%m-%d %H:%M:%S")

    # check if the new prediction time is later or earlier than the original time
    if new_prediction_time<original_prediction_time or new_prediction_time>original_prediction_time :

      # send to the next function if it the new prediction time is later or earlier
      if new_prediction_time>original_prediction_time:
        type_of_change = 'late'
      else:
        type_of_change = 'early'

      #### Sending An Update To The User Via Telegram ####
      send_message_to_user(type_of_change,chat_id,user_first_name,origin_stop_name,dest_stop_name,original_prediction_time, new_prediction_time)

      #### Sending The Request In Order To Turn the record as Not Active ####
      update_mongo_not_active(request_id)

      #### Inserting MongoDB With New Row ####
      insert_mongo_new_predicted_time(new_prediction_time_str , chat_id , user_id , origin_stop_id , destination_stop_id ,route_id,route_fare_class,route_name, trip_id , duration ,user_user_name
                             ,user_first_name, user_last_name , new_timestamp)