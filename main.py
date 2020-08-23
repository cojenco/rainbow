# [START rainbow_setup]
import sys
import base64
import json
import os
from google.cloud import vision
from google.cloud import pubsub_v1
from google.cloud import firestore

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
            'img_uri': uri,
            'event_id': event_id,
            'timestamp': timestamp,
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
    }

    # add data to Firestore with this data structure: users/{user_name}/meals/{event_id}/colors/{color_data}
    # can be replaced by a directory format?
    test_user_ref = db.collection('users').document('testUser1')
    meal_ref = test_user_ref.collection('meals').document('{}'.format(event_id))
    meal_ref.set(meal)
    color_ref = meal_ref.collection('colors').add(message)
    print('Did I arrive here?')
# [END functions_store_colors]
