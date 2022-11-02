##### KAFKA Connections #####
bootstrapServers = "Cnt7-naya-cdh63:9092"
topic1 = "mbta_bot"

##### MongoDB Connections #####
mongo_client_url = 'mongodb://localhost:27017/'
mongodb_client = "mbta"
mongodb_db = "mbta_bot"


###### TELEGRAM API #####
telegram_TOKEN = '5457489837:AAEz0ZFgb5KlV3Rtkwxlqk5TZrlzSSsLSqs'

###### GOOGLE API #####
google_maps_api_url = 'https://maps.googleapis.com/maps/api/'
GOOGLE_API_KEY = 'AIzaSyC-MHD-9xt-cosPBJaQTyBryiv9hUmalEg'

###### MBTA API #####
radius = '0.005'
mbta_api_url = 'https://api-v3.mbta.com/'
MBTA_API_KEY = '999fcb87560647fbab53d5ba694918b9'
route_type = {0:'Light Rail', 1:'Heavy Rail', 2:'Commuter Rail', 3:'Bus', 4:'Ferry'}