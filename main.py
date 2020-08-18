# [START rainbow_setup]
import sys
import base64
import json
import os
from google.cloud import vision
# [END rainbow_setup]


# [START functions_detect_color]
def detect_color(uri):
    print('Received URI: {}'.format(uri))
    client = vision.ImageAnnotatorClient()
    # image = vision.types.Image()
    # image.source.image_uri = uri

    response = client.image_properties({
        'source': {
            'image_uri': uri
        }
    })
    props = response.image_properties_annotation

    print('BELOW ARE DOMINANT COLORS:')
    for color in props.dominant_colors.colors:
        print('{}'.format(color))

    if response.error.message:
        raise Exception(
            '{}\nFor more info on error messages, check: '
            'https://cloud.google.com/apis/design/errors'.format(
                response.error.message))
# [END functions_detect_color]


# [START functions_process_img]
def process_img(event,context):
    # triggered by cloud storage event: google.storage.object.finalize
    bucket = event['bucket']
    filename = event['name']
    img_uri = 'gs://{}/{}'.format(bucket, filename)
    print('URI: {}'.format(img_uri))
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Created: {}'.format(event['timeCreated']))

    # call functions_detect_color
    detect_color(img_uri)

# [END functions_process_img]


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
