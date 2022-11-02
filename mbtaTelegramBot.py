import datetime
import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
from kafka import KafkaProducer
import json
import telebot
#import boto3
import googlemaps
import pytz
import configuration as c
import os

timezone = pytz.timezone("US/Eastern")

headers = CaseInsensitiveDict()
bot = telebot.TeleBot(c.telegram_TOKEN)
radius = c.radius
results_dict = {}
predicted_arrival_time = None
predicted_departure_time = None

###################### Start ############################
def start(message):
    request = message.text
    if request == "/start" or request == "start":
        return True
    else:
        return False


@bot.message_handler(func=start)
def start(message):
  msg = f'''Hi {message.from_user.username}!
  
I'm Massachusetts BayTransportation Authority BOT
I will help you to find the nearest transit options for your desired destination!
I can search for your transit by:

Type calc and then your origin place following a comma (',') and your destination place
For example: calc Harvard - Church St, Harvard Yard

And I will find the nearest arrival options for you.
You could choose the relevant trip you wish to track,
And I will send you an update if the predicted time will become earlier or later!

'''

  bot.send_message(message.chat.id, msg)


##################### Check the user input #######################
def check_request(message):
    request = message.text.split()
    msg = "Please follow the instructions - Type 'calc' and other search parameters"
    if request[0] not in "calc" and len(results_dict)==0:
        bot.send_message(message.chat.id, msg)
        return False
    if request[0] in "calc" and len(results_dict)==0 and ',' not in message.text:
        bot.send_message(message.chat.id, msg)
        return False
    elif len(request) < 3 and len(results_dict)==0:
        bot.send_message(message.chat.id, msg)
        return False
    elif len(request) < 3 and len(results_dict) !=0:
        check_input(message, results_dict)
    else:
        return True

### sending trip image to user ###
def send_trip_photo(message, origin_lat, origin_lng, dest_lat, dest_lng):
    service = 'directions'
    mode = 'transit'
    url = f"{c.google_maps_api_url}{service}/json?origin={origin_lat},{origin_lng}&destination={dest_lat},{dest_lng}&mode={mode}&key={c.GOOGLE_API_KEY}"

    # response from API
    payload = {}
    headers = {}

    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        respj = response.json()

        # create image
        img_path = rf'\tmp{message.chat.id}.png'
        img = open(img_path,'wb')
        encoded_polyline = respj['routes'][0]['overview_polyline']['points']
        img_url = f'{c.google_maps_api_url}staticmap?path=enc:{encoded_polyline}&size=800x800&key={c.GOOGLE_API_KEY}'
        img.write(requests.get(img_url).content)
        img.close()
        print(respj['routes'][0]['legs'][0]['distance']['text'])

        # send the photo to user
        trip_on_map_message = 'See below your requested trip on the map:'
        bot.send_message(message.chat.id, trip_on_map_message)
        bot.send_photo(message.chat.id, photo=open(img_path, 'rb'))

        # remove the img from the machine
        os.remove(img_path)

    except:
        return

def send_trip_to_user(message,  earliest_trips, origin_lat, origin_lng, dest_lat, dest_lng):
    i = 1
    # for each optional trip get the relevant fields and print to user
    for row in earliest_trips.itertuples():
        origin_stop_id =  row[earliest_trips.columns.get_loc('stop_id_x')+1]
        dest_stop_id =  row[earliest_trips.columns.get_loc('stop_id_y')+1]
        travel_time = row[earliest_trips.columns.get_loc('diff_between_times')+1]
        duration = row[earliest_trips.columns.get_loc('diff_between_times')+1]
        origin_arrival_time =  row[earliest_trips.columns.get_loc('arrival_time_x')+1]
        dest_arrival_time =  row[earliest_trips.columns.get_loc('arrival_time_y')+1]
        trip_id =  row[earliest_trips.columns.get_loc('trip_id')+1]
        route_id = row[earliest_trips.columns.get_loc('route_id_x')+1]
        route_description = row[earliest_trips.columns.get_loc('route_description_x')+1]
        route_fare_class  = row[earliest_trips.columns.get_loc('route_fare_class_x')+1]
        route_name  = row[earliest_trips.columns.get_loc('route_name_x')+1]
        origin_stop_sequence =  row[earliest_trips.columns.get_loc('stop_sequence_x')+1]
        dest_stop_sequence =  row[earliest_trips.columns.get_loc('stop_sequence_y')+1]
        origin_stop_name = row[earliest_trips.columns.get_loc('stop_name_x')+1]
        dest_stop_name = row[earliest_trips.columns.get_loc('stop_name_y')+1]
        msg = f'''*This is the {i} option -* 
              Your trip goes out in {route_fare_class}
              Line - {route_name}
              From {origin_stop_name} at {origin_arrival_time} 
              And will get to {dest_stop_name} in {dest_arrival_time}.
              Your time travel is {travel_time} minutes.'''
        results_dict[str(i)] = [trip_id, origin_stop_id, dest_stop_id, origin_stop_name,dest_stop_name,route_id,route_fare_class,route_name, duration, origin_arrival_time]
        i+=1
        bot.send_message(message.chat.id, msg, parse_mode= 'Markdown')
    send_trip_photo(message, origin_lat, origin_lng, dest_lat, dest_lng)
    msg = f'''Please enter your preferred selection number (like 1, 2, ...) you wish to track.
If you wish to re-start enter -1.'''

    bot.send_message(message.chat.id, msg)
    return

def check_input(message, results_dict):
    if message.text == '-1':
        return False
    if (message.text not in results_dict.keys()):
            msg = f'Please enter your preferred selection number (like 1, 2, ...) you wish to track.' \
                   f'If you wish to re-start enter -1.'
            bot.send_message(message.chat.id, msg)
    else:
        user_trip_selection(message, results_dict[message.text])


#### get the stop id's by the longtitude and latitude that were converted from the user request  ###
def get_stop_ids(message, dest_lng='', dest_lat='', origin_lng='', origin_lat='', origin_stop_id='', dest_stop_id='', radius=radius):
        from_url = f'{c.mbta_api_url}stops?filter[latitude]={origin_lat}&filter[longitude]={origin_lng}&filter[radius]={radius}&filter=api-key={c.MBTA_API_KEY}'
        resp = requests.get(from_url, headers=headers)

        # if there is bad request response
        if resp.status_code != 200:
            msg = 'I did not find any near stations for your request, try one more time'
            bot.send_message(message.chat.id, msg)
            return
        respj = resp.json()
        origin_stop_ids = []

        # if there is empty json
        if (len(respj['data'])) == 0:
            msg = 'I did not find any near origin stations for your request, try one more time'
            bot.send_message(message.chat.id, msg)
            return

        # for each origin station get the id and add to origin_stop_ids list
        for id in respj['data']:
            origin_stop_ids.append(id['id'])
        dest_url = f'{c.mbta_api_url}stops?filter[latitude]={dest_lat}&filter[longitude]={dest_lng}&filter[radius]={radius}&filter=api-key={c.MBTA_API_KEY}'
        resp = requests.get(dest_url, headers=headers)
        respj = resp.json()
        dest_stop_ids = []

        # if there is empty json
        if (len(respj['data'])) == 0:
            msg = 'I did not find any near destination stations for your request, try one more time'
            bot.send_message(message.chat.id, msg)
            return

        # for each dest station get the id and add to dest_stop_ids list
        for id in respj['data']:
            dest_stop_ids.append(id['id'])
        return origin_stop_ids, dest_stop_ids


# get the next schedules of the requested stops
def get_schedules(station_type,message, stop_ids, delta_time):
    if station_type == 'origin':
        delta_times = [100,150]
    if station_type == 'destination':
        delta_times = [100,150,200]
    for delta in delta_times:
        ### set the current time as min time and current time + provided delta as max time ###
        min_time = datetime.datetime.now(timezone).strftime("%H:%M")
        max_time = datetime.datetime.now(timezone) + datetime.timedelta(minutes=delta)
        max_time = max_time.strftime("%H:%M")

        # convert the stop_ids from list to concatenated string with commas
        stop_ids_strings = ','.join(stop_ids)
        from_url = f'{c.mbta_api_url}schedules?include=stop,route&filter[min_time]={min_time}&filter[max_time]={max_time}&filter[stop]={stop_ids_strings}&filter=api-key={c.MBTA_API_KEY}'
        resp = requests.get(from_url)
        respj = resp.json()

        if (len(respj['data'])) == 0 and delta!= 100:
            continue

        # if there is empty json
        if (len(respj['data'])) == 0:
            msg = 'I did not find any schedules of the stations for your request, try one more time'
            bot.send_message(message.chat.id, msg)
            return

        # initiate empty dataframe
        schedules = pd.DataFrame()

        i =0
        # for each relevant trip - get all the relevant data and add to dataframe
        for transit in respj['data']:
            arrival_time = transit['attributes']['arrival_time']
            departure_time = transit['attributes']['departure_time']
            if arrival_time:
                arrival_time = arrival_time[:10] + " " + arrival_time[11:19]
                arrival_time = datetime.datetime.strptime(arrival_time, '%Y-%m-%d %H:%M:%S')
            else:
                arrival_time = departure_time[:10] + " " + departure_time[11:19]
                arrival_time = datetime.datetime.strptime(arrival_time, '%Y-%m-%d %H:%M:%S')
            stop_sequence = transit['attributes']['stop_sequence']
            route_id = transit['relationships']['route']['data']['id']
            stop_id = transit['relationships']['stop']['data']['id']
            stop_name = ''
            route_description = ''
            route_fare_class = ''
            route_name = ''
            # for each stop, get the stop name from the included stop
            for included in respj['included']:
                if included['type']=='stop':
                    if included['id'] == stop_id:
                        stop_name = included['attributes']['name']
                if included['type'] == 'route':
                    if included['id'] == route_id:
                        route_description = included['attributes']['description']
                        route_fare_class = included['attributes']['fare_class']
                        route_name =  included['attributes']['short_name'] + ' ' + included['attributes']['long_name']
            trip_id = transit['relationships']['trip']['data']['id']
            new_row = {'arrival_time':arrival_time, 'stop_sequence':int(stop_sequence), 'route_id':route_id,
                      'route_description':route_description,
                       'route_fare_class':route_fare_class,
                       'route_name':route_name,
                       'stop_id':stop_id,'stop_name':stop_name, 'trip_id':trip_id}
            df_dictionary = pd.DataFrame([new_row])
            schedules = pd.concat([schedules, df_dictionary], ignore_index=True)
            i+=1
        schedules = schedules.drop_duplicates()
        return schedules

### from the earlliest trips dataframe - set the diff_between departuare and arrival time in minutes, filter by stop_sequence and sort by the arrival time
def get_earliest_trips(message, joined_schedules):
    joined_schedules['diff_between_times'] = (joined_schedules.arrival_time_y - joined_schedules.arrival_time_x) / pd.Timedelta(minutes=1)
    joined_schedules = joined_schedules[joined_schedules['stop_sequence_x'] < joined_schedules['stop_sequence_y']]
    selected_trip = joined_schedules[joined_schedules.diff_between_times == joined_schedules.diff_between_times.min()].drop_duplicates()
    selected_trip = selected_trip.sort_values('arrival_time_x')
    if selected_trip.shape[0] == 0:
        msg = 'I did not find any trip between the stations for your request, try one more time'
        bot.send_message(message.chat.id, msg)
        return
    return selected_trip

### the main function for getting the trip by the user request ###
@bot.message_handler(func=check_request)
def calculate_best_trip(message):
    locations = message.text.split('calc')[1].split(',')
    origin_location = locations[0]
    dest_location = locations[1]
    chat_id = message.chat.id

    # create Gmap client
    gmaps = googlemaps.Client(key=c.GOOGLE_API_KEY)

    # Geocoding an address - origin
    origin_geocode_result = gmaps.geocode(origin_location)
    if not origin_geocode_result:
        msg = 'I did not find any origin location per your request, try one more time'
        bot.send_message(message.chat.id, msg)
        return
    origin_lat = origin_geocode_result[0]['geometry']['location']['lat']
    origin_lng = origin_geocode_result[0]['geometry']['location']['lng']

    # Geocoding an address - destination
    destination_geocode_result = gmaps.geocode(dest_location)
    if not destination_geocode_result:
        msg = 'I did not find any destination location per your request, try one more time'
        bot.send_message(message.chat.id, msg)
        return
    destination_lat = destination_geocode_result[0]['geometry']['location']['lat']
    destination_lng = destination_geocode_result[0]['geometry']['location']['lng']

    # getting the stop id's as list
    stop_ids = get_stop_ids(message, origin_lng=origin_lng, origin_lat=origin_lat, dest_lat=destination_lat, dest_lng=destination_lng)

    #### Error or empty response handling - if didn't return any stop id's ####
    if stop_ids is None:
        return
    origin_stop_ids = stop_ids[0]
    dest_stop_ids = stop_ids[1]
    origin_schedules = get_schedules('origin', message, origin_stop_ids, delta_time=100)
    dest_schedules = get_schedules('destination', message, dest_stop_ids, delta_time=100)

    #### Error or empty response handling - if didn't find any schedules from origin or destination ####
    if origin_schedules is None or dest_schedules is None:
        return

    joined_schedules = origin_schedules.merge(dest_schedules, on='trip_id')
    #### Error or empty response handling - if didnt find schedules from the origin to the destination ####
    if joined_schedules.shape[0] == 0:
        msg = 'I did not find any schedules between the stations for your request, try one more time'
        bot.send_message(message.chat.id, msg)
        return
    else:
        earliest_trips = get_earliest_trips(message, joined_schedules)
    #### Error or empty response handling ####
    if earliest_trips is None:
        return
    send_trip_to_user(message, earliest_trips, origin_lat, origin_lng, destination_lat, destination_lng)


def get_trip_prediction(message, trip_id, origin_stop_id, origin_scheduled_time):
    # make API request
    from_url = f'{c.mbta_api_url}predictions?filter[trip]={trip_id}&filter[stop]={origin_stop_id}&filter=api-key={c.MBTA_API_KEY}'
    resp = requests.get(from_url, headers=headers)

    # if there is bad request response
    if resp.status_code != 200:
        msg = 'I did not find any real-time predictions for this trip. please try again'
        bot.send_message(message.chat.id, msg)
        return
    respj = resp.json()

    # if there is empty json
    if (len(respj['data'])) == 0:
        msg = 'I did not find any real-time predictions for this trip. please try again'
        bot.send_message(message.chat.id, msg)
        return

    if respj["data"][0]["attributes"]["arrival_time"] is not None:
        predicted_arrival_time = respj["data"][0]["attributes"]["arrival_time"]
        predicted_arrival_time = predicted_arrival_time[:10] + " " + predicted_arrival_time[11:19]
    else:
        predicted_arrival_time = ''

    if respj["data"][0]["attributes"]["departure_time"] is not None:
        predicted_departure_time = respj["data"][0]["attributes"]["departure_time"]
        predicted_departure_time = predicted_departure_time[:10] + " " + predicted_departure_time[11:19]
    else:
        predicted_departure_time = ''
    if predicted_arrival_time == '' and predicted_departure_time != '':
        predicted_arrival_time = predicted_departure_time
    if predicted_arrival_time == '' and predicted_departure_time == '':
        predicted_arrival_time = origin_scheduled_time
        predicted_departure_time = origin_scheduled_time

    return predicted_arrival_time, predicted_departure_time

def get_alerts(message,trip_id,origin_stop_id):
    # make API request
    from_url = f'{c.mbta_api_url}alerts?filter[trip]={trip_id}&filter[stop]={origin_stop_id}&filter=api-key={c.MBTA_API_KEY}'
    resp = requests.get(from_url, headers=headers)

    # if there is bad request response
    if resp.status_code != 200:
        return None
    respj = resp.json()

    # if there is empty json
    if (len(respj['data'])) == 0:
        return None

    if respj["data"][0]["attributes"]["active_period"][0] is not None:
            attributes = respj["data"][0]["attributes"]
            active_period = attributes['active_period']
            if active_period[0]['start'] is not None and active_period[0]['end'] is None:
                return None
            start_date = active_period[0]['start'][:10]
            start_date =  datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end_date = active_period[0]['end'][:10]
            end_date =  datetime.datetime.strptime(end_date, '%Y-%m-%d')
            current_time = datetime.datetime.today()
            current_time = current_time.strftime('%Y-%m-%d')
            current_time =  datetime.datetime.strptime(current_time, '%Y-%m-%d')
            if current_time > start_date and end_date > current_time:
                start_date = active_period[0]['start']
                end_date = active_period[0]['end']
                header = attributes['header']
                service_effect = attributes['service_effect']
                effect = attributes['effect']
                cause = attributes['cause']
                dict = {'start_date':start_date,'end_date':end_date, 'header':header,'service_effect':service_effect, 'effect':effect, 'cause':cause }
                return dict
            else:
                return None

# get the selected trip of the user
def user_trip_selection(message, results_dict):
    trip_id = results_dict[0]
    origin_stop_id = results_dict[1]
    dest_stop_id = results_dict[2]
    origin_stop_name =  results_dict[3]
    dest_stop_name =  results_dict[4]
    route_id = results_dict[5]
    route_fare_class = results_dict[6]
    route_name = results_dict[7]
    duration = results_dict[8]
    origin_scheduled_time = results_dict[9]
    origin_scheduled_time = origin_scheduled_time.strftime('%Y-%m-%d %H:%M:%S')

    # get alerts
    alerts = get_alerts(message, trip_id, origin_stop_id)

    if alerts is not None:
        msg = f'''Please note the following alert for your origin stop and trip:
    Cause: {alerts['cause']}
    Effect: {alerts['effect']}

    Description:
    *{alerts['header']}.*
    '''
        bot.send_message(message.chat.id, msg, parse_mode='Markdown')

    # call to the function that gets the predicted departure & arrival time
    try:
        predicted_arrival_time, predicted_departure_time = get_trip_prediction(message, trip_id, origin_stop_id, origin_scheduled_time)
    except:
        return

    # send the message to the user with the relevant trip details
    msg_to_user = f'''*The predicted arrival time for your chosen trip: 
                  from {origin_stop_name} at {predicted_arrival_time} 
                  and departure time is at {predicted_departure_time}.*'''
    bot.send_message(message.chat.id, msg_to_user,parse_mode= 'Markdown')


    # get variables from session
    user_id = message.from_user.id
    user_user_name = message.from_user.username
    user_first_name = message.from_user.first_name
    user_last_name = message.from_user.last_name
    chat_id = message.chat.id

    # prepare the dictionary with the trip details to be sent on kafka
    data_dict = {'predicted_arrival_time': predicted_arrival_time,'chat_id': chat_id, 'user_id': user_id, 'origin_stop_id':origin_stop_id,'destination_stop_id':dest_stop_id,
                 'origin_stop_name':origin_stop_name, 'dest_stop_name':dest_stop_name,
                 'route_id':route_id,'route_fare_class':route_fare_class,'route_name':route_name,
                 'trip_id':trip_id, 'duration': duration,'user_user_name': user_user_name,'user_first_name': user_first_name, 'user_last_name': user_last_name}

    # normalize the semi-structured JSON into a flat table
    df = pd.json_normalize(data_dict)
    send_to_kafka(df)


def send_to_kafka(df):
    # Topics/Brokers
    topic1 = c.topic1
    brokers = [c.bootstrapServers]
    producer = KafkaProducer(bootstrap_servers=brokers)

    #### Getting the data ready for kafka - convert the flat table to dict####
    row = df.to_dict(orient='records')[0]
    row_json_str = json.dumps(row)
    producer.send(topic1, value=row_json_str.encode('utf-8'))
    producer.flush()

bot.polling()
