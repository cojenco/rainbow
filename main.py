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
        topic_name = 'projects/keen-boulder-286521/topics/TopicColorsDetected'
    
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
    # background cloud cunction to be triggered by Pub/Sub.
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
    meal_ref = db.collection('meals').document('{}'.format(event_id))
    meal_ref.set(meal)
    color_ref = meal_ref.collection('colors').add(message)
    print('Did I arrive here?')
# [END functions_store_colors]


# [START functions_retrieve_colors][ENTRY POINT for retrieve_colors]
def retrieve_colors(event, context):
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
    
    # query Firestore: retrieve meal docs from users/{'uID'}/meals collection
    # collections = db.collection('users').document('{}'.format(uID)).collections()
    # for collection in collections:
    #     for doc in collection.stream():
    #         print(f'{doc.id} => {doc.to_dict()}')
    event_id = event['attributes']['event_id']
    print('{}'.format(event_id))
    collections = db.collection('meals').document('{}'.format(event_id)).collections()
    print(' collections: {} '.format(collections))
    for collection in collections:
        print(' collection: {} '.format(collection))
        for doc in collection.stream():
            print(f'{doc.id} => {doc.to_dict()}')

    # query = db.collection_group(u'colors').where(u'event_id', u'==', u'1454537197728696')
    #     # .where(u'timestamp', u'>', u'2020-08-23T20:00:44.411Z')
    # print('Here Here?')
    # print(' {} '.format(query))
    
    # user = db.collection(u'users').where(u'uID', u'==', u'{}'.format(message))
    # meals = user.collection(u'meals').where(u'timestamp', u'>', u'2020-08-23T20:00:44.411Z')
    ### user = db.collection(u'users').where(u'uID', u'==', u'testUser1').stream()
    ### print(' {} '.format(user))
    # meals = user.collection(u'meals').where(u'event_id', u'==', u'1454537197728696')
    print('here here?!')
# [END functions_retrieve_colors][ENTRY POINT for retrieve_colors]


# [START functions_get_daterange_colors][ENTRY POINT for get_daterange_colors]
def get_daterange_colors(event, context):
    # background cloud cunction to be triggered by Pub/Sub topic: TopicGetDaterangeColors
    print('Get colors by daterange')
    print(' {} '.format(event))
    print('BELOW IS CONTEXT')
    print(' {} '.format(context))
    # topic_timestamp = context['timestamp']
    utc_now = datetime.now(timezone.utc)
    dt = utc_now - timedelta(7)
    start_time = u'{}'.format(dt)
    end_time = u'{}'.format(utc_now)
    print(start_time)
    print(end_time)

    
    uID = 'testUser10'
    # uID = event['attributes']['uID']
    # event_id = event['attributes']['event_id']
    # print('{}'.format(event_id))

    meals_ref = db.collection(u'meals').where(u'uID', u'==', uID)
    print('{}'.format(meals_ref))
    daterange_meals = meals_ref.where(u'timestamp', u'>=', start_time).where(u'timestamp', u'<=', end_time)
    print('{}'.format(daterange_meals))


    # collections = db.collection('meals').document('{}'.format(event_id)).collections()
    # print(' collections: {} '.format(collections))
    # for collection in collections:
    #     print(' collection: {} '.format(collection))
    #     for doc in collection.stream():
    #         print(f'{doc.id} => {doc.to_dict()}')

    # query = db.collection_group(u'colors').where(u'event_id', u'==', u'1454537197728696')
    #     # .where(u'timestamp', u'>', u'2020-08-23T20:00:44.411Z')
    # print('Here Here?')
    # print(' {} '.format(query))
    
    # user = db.collection(u'users').where(u'uID', u'==', u'{}'.format(message))
    # meals = user.collection(u'meals').where(u'timestamp', u'>', u'2020-08-23T20:00:44.411Z')
    ### user = db.collection(u'users').where(u'uID', u'==', u'testUser1').stream()
    ### print(' {} '.format(user))
    # meals = user.collection(u'meals').where(u'event_id', u'==', u'1454537197728696')
    print('here here?!')
# [END functions_get_daterange_colors][ENTRY POINT for get_daterange_colors]
