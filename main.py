# [START rainbow_setup]
import sys
import base64
import json
import os
from google.cloud import vision
from google.cloud import pubsub_v1
from google.cloud import firestore
# export GOOGLE_APPLICATION_CREDENTIALS="/home/Users/cathyo/Downloads/My First Project-59cc3328ec06.json"
# [END rainbow_setup]


# [START functions_detect_color][Called in img-colors-extract]
def detect_color(uri, timestamp, event_id=0):
    print('Received URI: {}'.format(uri))
    client = vision.ImageAnnotatorClient()
    image = vision.types.Image()
    image.source.image_uri = uri
    response = client.image_properties(image=image)
    # use client library image_properties kwargs equivalent as below (https://googleapis.dev/python/vision/latest/gapic/v1/api.html)
    # response = client.image_properties({
    #     'source': {
    #         'image_uri': uri
    #     }
    # })

    props = response.image_properties_annotation
    colors_array = props.dominant_colors.colors
    db = firestore.Client()

    # Store dominant colors: Add data/documents to Firestore
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
        db.collection('meals').add(data)
    # End of For loop 

    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(response.error.message))
    else:
        publish_colors_detected(uri, colors_array)
# [END functions_detect_color]


# [START functions_publish_colors_detected][Called in img-colors-extract]
def publish_colors_detected(uri, colors_array):
    print('Arrived at publish_colors_detected with {}'.format(uri))
    print('{}'.format(colors_array))
    publisher = pubsub_v1.PublisherClient()
    # topic_name = 'projects/{project_id}/topics/{topic}'.format(
    #     project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
    #     topic='TopicColorsDetected',
    # )
    topic_name = 'projects/keen-boulder-286521/topics/TopicColorsDetected'
    
    future = publisher.publish(topic_name, b'Rainbow!')
    message_id = future.result()
    print(' {} '.format(message_id))
# [END functions_publish_colors_detected]


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
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Created: {}'.format(event['timeCreated']))

    # call functions_detect_color
    detect_color(img_uri, timestamp, event_id)
# [END functions_process_img]


# [START functions_store_colors][ENTRY POINT for store_colors]
def store_colors(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    print('RAINBOW WIP! Arrived at subscriber FUNCTION store_colors')
    print("""This Function was triggered by messageId {} published at {}
    """.format(context.event_id, context.timestamp))

    # To be deleted and re-deployed: write to Firestore
    db = firestore.Client()
    doc_ref = db.collection('cities').document('houston')
    doc_ref.set({
        'name': 'Houston',
        'state': 'Texas',
        'country': 'USA'
    })
    print('Did I arrive here?')
# [END functions_store_colors]


# [START functions_store_color_docs][Called in img-colors-extract]
# def store_color_docs():
# [END functions_store_color_docs]


# [START functions_detect_colors]
# def detect_colors(event,context):
#     # triggered by cloud storage event: google.storage.object.finalize
#     from google.cloud import vision
#     bucket = event['bucket']
#     filename = event['name']
#     img_uri = 'gs://{}/{}'.format(bucket, filename)
#     print('URI: {}'.format(img_uri))
#     print('Self Link: {}'.format(event['selfLink']))
#     print('Event ID: {}'.format(context.event_id))
#     print('Event type: {}'.format(context.event_type))
#     print('Bucket: {}'.format(event['bucket']))
#     print('File: {}'.format(event['name']))
#     print('Created: {}'.format(event['timeCreated']))
#     # send image uri to vision client to detect colors
#     print('File sent vision API to detect colors!')
#     client = vision.ImageAnnotatorClient()
#     response = client.image_properties({
#         'source': {
#             'image_uri': img_uri
#         }
#     })
#     props = response.image_properties_annotation
#     # extract dominant colors
#     print('BELOW ARE DOMINANT COLORS:')
#     for color in props.dominant_colors.colors:
#         print('{}'.format(color))
# [END functions_detect_colors]