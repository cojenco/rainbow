# [START rainbow_setup]
import sys
import base64
import json
import os
from google.cloud import vision
from google.cloud import pubsub_v1
from google.cloud import firestore
from datetime import datetime, timezone, timedelta

client = vision.ImageAnnotatorClient()
publisher = pubsub_v1.PublisherClient()
db = firestore.Client()
# [END rainbow_setup]



########## DEPLOYED FUNCTIONS ################################
# [START functions_entry_get_image][ENTRY POINT for findColor]
def entry_get_image(event,context):
    # triggered by cloud storage: bucket_rainbow_meals
    # triggered by cloud storage event: google.storage.object.finalize
    bucket = event['bucket']
    filename = event['name']
    timestamp = event['timeCreated']
    event_id = context.event_id
    img_uri = 'gs://{}/{}'.format(bucket, filename)
    print('URI: {}'.format(img_uri))
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Created: {}'.format(event['timeCreated']))

    # get blob metadata
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.get_blob(filename)
    print("Get Metadata: {}".format(blob.metadata))
    uID = blob.metadata['uID']
    print("Get uID: {}".format(uID))

    # call functions_process_color
    process_color(img_uri, uID, timestamp, event_id)
# [END functions_entry_get_image][ENTRY POINT for findColor]


# [START functions_process_color][Called in findColor]
def process_color(uri, uID, timestamp, event_id=0):
    print('Received URI: {}'.format(uri))
    print('Received uID: {}'.format(uID))

    response = client.image_properties({
          'source': {
               'image_uri': uri
          }
    })

    props = response.image_properties_annotation
    colors_array = props.dominant_colors.colors

    # retrieve dominant colors and encode color data into a base64-encoded byte string
    # publish to topic "TopicColorsDetected" in pub/sub
    for color in props.dominant_colors.colors:
        # print('{}'.format(color))
        data = {
            'red': color.color.red,
            'green': color.color.green,
            'blue': color.color.blue,
            'score': color.score,
            'pixel_fraction': color.pixel_fraction,
            'event_id': event_id,
            'timestamp': timestamp,
            'img_uri': uri,
            'uID': uID,
        }

        message_data = json.dumps(data).encode('utf-8')
        topic_name = 'projects/keen-boulder-286521/topics/TopicColorsFound' #REVISE using env var
    
        future = publisher.publish(topic_name, data=message_data)
        message_id = future.result()
        print(' {} '.format(message_id))
    # End of For loop 

    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(response.error.message))
# [END functions_process_color][Called in findColor]



# [START functions_firestore_colors][ENTRY POINT for firestore_colors]
def firestore_colors(event, context):
    # background cloud cunction to be triggered by Pub/Sub topic: TopicColorsFound
    print('Arrived at subscriber FUNCTION firestore_colors')

    if event.get('data'):
        message_data = base64.b64decode(event['data']).decode('utf-8')
        message = json.loads(message_data)
    else:
        raise ValueError('Data sector is missing in the Pub/Sub message.')

    # organize data for Firestore
    event_id = message['event_id']
    uID = message['uID']
    # print(' {} '.format(event_id))
    meal = {
        'img_uri': message['img_uri'],
        'event_id': event_id,
        'timestamp': message['timestamp'],
        'uID': uID
    }

    user = {
        'uID': uID,
     #    'email': 'testUser10@gmail.com',
    }

    # add data to Firestore with this data structure: users/{user_name}/meals/{event_id}/colors/{color_data}
    # can be replaced by a directory format?
    test_user_ref = db.collection('users').document('{}'.format(uID))
    test_user_ref.set(user)
    meal_ref = test_user_ref.collection('meals').document('{}'.format(event_id))
    meal_ref.set(meal)
    color_ref = meal_ref.collection('colors').add(message)
    print('Saved to Firestore')
# [END functions_firestore_colors]

# [START functions_call_weekly_colors][ENTRY POINT for call_weekly_colors]
def callWeeklyColors(request):
    # Set CORS headers for the preflight request
    if request.method == 'OPTIONS':
        # Allows POST requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)

    # Set CORS headers for the main request
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    uID = '112304765224766018668'
    request_json = request.get_json()
    if request.args and 'message' in request.args:
        uID = request.args.get('message')
        print('ARGS')
        print(' {} '.format(uID))
        # return request.args.get('message')
    elif request_json and 'message' in request_json:
        print('request_json')
        uID = request_json['message']
        print(' {} '.format(uID))
        # return request_json['message']
    else:
        print('Hello World 200!')
        # return ('Hello World!', 200, headers)

    # query Firestore: filter meals within the past week
    # query Firestore: retrieve meal docs from users/{'uID'}/meals collection
    print('{}'.format(uID))
    utc_now = datetime.now(timezone.utc)
    dt = utc_now - timedelta(7)
    start_time = u'{}'.format(dt)
    end_time = u'{}'.format(utc_now)
    all_colors = []

    meals_ref = db.collection('users').document('{}'.format(uID)).collection('meals').where(u'timestamp', u'>=', start_time).where(u'timestamp', u'<=', end_time)
    meals = meals_ref.stream()
    for doc in meals:
        print(f'{doc.id} => {doc.to_dict()}')
        event_id = f'{doc.id}'
        dish = get_dish_colors(uID, event_id)
        all_colors.extend(dish)

    print(all_colors)
    # data = {
    #     'colors': all_colors,
    # }
    # response = make_response(data)
    response = jsonify({"colors": all_colors})
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response
# [END functions_call_weekly_colors][ENTRY POINT for call_weekly_colors]

# [START functions_get_dish_colors][Called in call_weekly_colors]
def get_dish_colors(uID, event_id):
     colors = []  
     collections = db.collection('users').document('{}'.format(uID)).collection('meals').document('{}'.format(event_id)).collections()
     for collection in collections:
          for doc in collection.stream():
               color = doc.to_dict()
               colors.append(color)
          #  print(f'{doc.id} => {doc.to_dict()}')
     print('Here is a list of colors')  
     return colors 
# [END functions_get_dish_colors][Called in call_weekly_colors]
########## DEPLOYED FUNCTIONS ################################



########## ORIGINAL FUNCTIONS ################################
# [START functions_detect_color][Called in img-colors-extract]
def detect_color(uri, timestamp, event_id=0):
    print('Received URI: {}'.format(uri))
    image = vision.types.Image()
    image.source.image_uri = uri
    response = client.image_properties(image=image)

    props = response.image_properties_annotation
    colors_array = props.dominant_colors.colors

    # retrieve dominant colors and encode color data into a base64-encoded byte string
    # publish to topic "TopicColorsDetected" in pub/sub
    for color in props.dominant_colors.colors:
        # print('{}'.format(color))
        data = {
            'red': color.color.red,
            'green': color.color.green,
            'blue': color.color.blue,
            'score': color.score,
            'pixel_fraction': color.pixel_fraction,
            'event_id': event_id,
            'timestamp': timestamp,
            'img_uri': uri,
            'uID': 'testUser10',      #TBD
        }

        message_data = json.dumps(data).encode('utf-8')
        topic_name = 'projects/keen-boulder-286521/topics/TopicColorsDetected' #REVISE using env var
    
        future = publisher.publish(topic_name, data=message_data)
        message_id = future.result()
        print(' {} '.format(message_id))
    # End of For loop 

    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(response.error.message))
# [END functions_detect_color]


# [START functions_process_img][ENTRY POINT for img-colors-extract]
def process_img(event,context):
    # triggered by cloud storage event: google.storage.object.finalize
    bucket = event['bucket']
    filename = event['name']
    timestamp = event['timeCreated']
    event_id = context.event_id
    img_uri = 'gs://{}/{}'.format(bucket, filename)
    print('URI: {}'.format(img_uri))
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Created: {}'.format(event['timeCreated']))

    # call functions_detect_color
    detect_color(img_uri, timestamp, event_id)
# [END functions_process_img]


# [START functions_store_colors][ENTRY POINT for store_colors]
def store_colors(event, context):
    # background cloud cunction to be triggered by Pub/Sub topic: TopicColorsDetected
    print('RAINBOW WIP! Arrived at subscriber FUNCTION store_colors')
    # print(' {} '.format(event))

    if event.get('data'):
        message_data = base64.b64decode(event['data']).decode('utf-8')
        message = json.loads(message_data)
    else:
        raise ValueError('Data sector is missing in the Pub/Sub message.')

    # organize data for Firestore
    print(' {} '.format(message))
    event_id = message['event_id']
    # print(' {} '.format(event_id))
    meal = {
        'img_uri': message['img_uri'],
        'event_id': event_id,
        'timestamp': message['timestamp'],
        'uID': 'testUser10'      #TBD
    }

    user = {
        'email': 'testUser10@gmail.com',
        'uID': 'testUser10',
    }

    # add data to Firestore with this data structure: users/{user_name}/meals/{event_id}/colors/{color_data}
    # can be replaced by a directory format?
    test_user_ref = db.collection('users').document('testUser10')
    test_user_ref.set(user)
    meal_ref = test_user_ref.collection('meals').document('{}'.format(event_id))
    meal_ref.set(meal)
    color_ref = meal_ref.collection('colors').add(message)
    print('Did I arrive here?')
# [END functions_store_colors]


# [START functions_get_weekly_colors][ENTRY POINT for get_weekly_colors]
def get_weekly_colors(event, context):
    # background cloud cunction to be triggered by Pub/Sub topic: TopicRetrieveColors
    print('Retrieving colors from Firestore')
    print(' {} '.format(event))
    print('BELOW IS CONTEXT')
    print(' {} '.format(context))
    uID = 'testUser1'

    if event.get('data'):
        uID = base64.b64decode(event['data']).decode('utf-8')
        print(' {} '.format(uID))
    else:
        print('Data sector is missing in the Pub/Sub message')
    
    # query Firestore: filter meals within the past week
    # query Firestore: retrieve meal docs from users/{'uID'}/meals collection
    utc_now = datetime.now(timezone.utc)
    dt = utc_now - timedelta(7)
    start_time = u'{}'.format(dt)
    end_time = u'{}'.format(utc_now)
    all_colors = []

    meals_ref = db.collection('users').document('{}'.format(uID)).collection('meals').where(u'timestamp', u'>=', start_time).where(u'timestamp', u'<=', end_time)
    meals = meals_ref.stream()
    for doc in meals:
        # print(f'{doc.id} => {doc.to_dict()}')
        event_id = f'{doc.id}'
        dish = get_dish_colors(uID, event_id)
        all_colors.extend(dish)

    print(all_colors)
# [END functions_get_weekly_colors][ENTRY POINT for get_weekly_colors]


# [START functions_get_dish_colors][Called in get_weekly_colors]
def get_dish_colors(uID, event_id):
     colors = []  
     collections = db.collection('users').document('{}'.format(uID)).collection('meals').document('{}'.format(event_id)).collections()
     for collection in collections:
          for doc in collection.stream():
               color = doc.to_dict()
               colors.append(color)
          #  print(f'{doc.id} => {doc.to_dict()}')
     print('Here is a list of colors')  
     return colors 
# [END functions_get_dish_colors][Called in get_weekly_colors]
