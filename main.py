import sys

# [START functions_helloworld_http]
# [START functions_http_content]
from flask import escape

# [END functions_helloworld_http]
# [END functions_http_content]

# [START functions_rainbow_storage]
def detect_color(event,context):
  # triggered by cloud storage event: google.storage.object.finalize
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Created: {}'.format(event['timeCreated']))

    
# [END functions_rainbow_storage]