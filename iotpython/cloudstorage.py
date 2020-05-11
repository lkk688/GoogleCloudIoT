#!/usr/bin/env python

from google.cloud import storage

import paho.mqtt.client as mqtt
import jwt
import time
import ssl
import random
import os
import logging
import datetime
import argparse

import random
import time
import datetime
import json


logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.CRITICAL)

# The initial backoff time after a disconnection occurs, in seconds.
minimum_backoff_time = 1

# The maximum backoff time before giving up, in seconds.
MAXIMUM_BACKOFF_TIME = 32

# Whether to wait with exponential backoff before publishing.
should_backoff = False

# [START iot_mqtt_jwt]


def create_jwt(project_id, private_key_file, algorithm):
    """Creates a JWT (https://jwt.io) to establish an MQTT connection.
        Args:
         project_id: The cloud project ID this device belongs to
         private_key_file: A path to a file containing either an RSA256 or
                 ES256 private key.
         algorithm: The encryption algorithm to use. Either 'RS256' or 'ES256'
        Returns:
            A JWT generated from the given project_id and private key, which
            expires in 20 minutes. After 20 minutes, your client will be
            disconnected, and a new JWT will have to be generated.
        Raises:
            ValueError: If the private_key_file does not contain a known key.
        """

    token = {
        # The time that the token was issued at
        'iat': datetime.datetime.utcnow(),
        # The time the token expires.
        'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=20),
        # The audience field should always be set to the GCP project id.
        'aud': project_id
    }

    # Read the private key file.
    with open(private_key_file, 'r') as f:
        private_key = f.read()

    print('Creating JWT using {} from private key file {}'.format(
        algorithm, private_key_file))

    return jwt.encode(token, private_key, algorithm=algorithm)
# [END iot_mqtt_jwt]

# [START iot_mqtt_config]


def error_str(rc):
    """Convert a Paho error to a human readable string."""
    return '{}: {}'.format(rc, mqtt.error_string(rc))


def on_connect(unused_client, unused_userdata, unused_flags, rc):
    """Callback for when a device connects."""
    print('on_connect', mqtt.connack_string(rc))

    # After a successful connect, reset backoff time and stop backing off.
    global should_backoff
    global minimum_backoff_time
    should_backoff = False
    minimum_backoff_time = 1


def on_disconnect(unused_client, unused_userdata, rc):
    """Paho callback for when a device disconnects."""
    print('on_disconnect', error_str(rc))

    # Since a disconnect occurred, the next loop iteration will wait with
    # exponential backoff.
    global should_backoff
    should_backoff = True


def on_publish(unused_client, unused_userdata, unused_mid):
    """Paho callback when a message is sent to the broker."""
    print('on_publish')


def on_message(unused_client, unused_userdata, message):
    """Callback when the device receives a message on a subscription."""
    payload = str(message.payload.decode('utf-8'))
    print('Received message \'{}\' on topic \'{}\' with Qos {}'.format(
        payload, message.topic, str(message.qos)))


def get_client(
        project_id, cloud_region, registry_id, device_id, private_key_file,
        algorithm, ca_certs, mqtt_bridge_hostname, mqtt_bridge_port):
    """Create our MQTT client. The client_id is a unique string that identifies
    this device. For Google Cloud IoT Core, it must be in the format below."""
    client_id = 'projects/{}/locations/{}/registries/{}/devices/{}'.format(
        project_id, cloud_region, registry_id, device_id)
    print('Device client_id is \'{}\''.format(client_id))

    client = mqtt.Client(client_id=client_id)

    # With Google Cloud IoT Core, the username field is ignored, and the
    # password field is used to transmit a JWT to authorize the device.
    client.username_pw_set(
        username='unused',
        password=create_jwt(
            project_id, private_key_file, algorithm))

    # Enable SSL/TLS support.
    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    # Register message callbacks. https://eclipse.org/paho/clients/python/docs/
    # describes additional callbacks that Paho supports. In this example, the
    # callbacks just print to standard out.
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # Connect to the Google MQTT bridge.
    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    # This is the topic that the device will receive configuration updates on.
    mqtt_config_topic = '/devices/{}/config'.format(device_id)

    # Subscribe to the config topic.
    client.subscribe(mqtt_config_topic, qos=1)

    # The topic that the device will receive commands on.
    mqtt_command_topic = '/devices/{}/commands/#'.format(device_id)

    # Subscribe to the commands topic, QoS 1 enables message acknowledgement.
    print('Subscribing to {}'.format(mqtt_command_topic))
    client.subscribe(mqtt_command_topic, qos=0)

    return client
# [END iot_mqtt_config]

def cloudstorage_demo(args):
    # Instantiates a google cloud client
    storage_client = storage.Client()
    #upload image to an existing bucket
    bucketexist = storage_client.bucket('cmpelkk_imagetest')
    blobexist = bucketexist.blob('sjsu/img1.jpg')

    for filename in os.listdir('./data'):
        if filename.endswith(".jpg"): 
            print(filename)#os.path.join(directory, filename))
            blobexist.upload_from_filename(filename)

            # Process network events.
            client.loop()

            continue
        else:
            continue

def mqtt_device_demo(args):

    """Connects a device, sends data, and receives data."""
    # [START iot_mqtt_run]
    global minimum_backoff_time
    global MAXIMUM_BACKOFF_TIME

    # Publish to the events or state topic based on the flag.
    sub_topic = 'events' if args.message_type == 'event' else 'state'

    mqtt_topic = '/devices/{}/{}'.format(args.device_id, sub_topic)

    jwt_iat = datetime.datetime.utcnow()
    jwt_exp_mins = args.jwt_expires_minutes
    client = get_client(
        args.project_id, args.cloud_region, args.registry_id,
        args.device_id, args.private_key_file, args.algorithm,
        args.ca_certs, args.mqtt_bridge_hostname, args.mqtt_bridge_port)

    # Publish num_messages messages to the MQTT bridge once per second.
    for i in range(1, args.num_messages + 1):
        # Process network events.
        client.loop()

        # Wait if backoff is required.
        if should_backoff:
            # If backoff time is too large, give up.
            if minimum_backoff_time > MAXIMUM_BACKOFF_TIME:
                print('Exceeded maximum backoff time. Giving up.')
                break

            # Otherwise, wait and connect again.
            delay = minimum_backoff_time + random.randint(0, 1000) / 1000.0
            print('Waiting for {} before reconnecting.'.format(delay))
            time.sleep(delay)
            minimum_backoff_time *= 2
            client.connect(args.mqtt_bridge_hostname, args.mqtt_bridge_port)

        payload = '{}/{}-payload-{}'.format(
            args.registry_id, args.device_id, i)
        print('Publishing message {}/{}: \'{}\''.format(
            i, args.num_messages, payload))
        # [START iot_mqtt_jwt_refresh]
        seconds_since_issue = (datetime.datetime.utcnow() - jwt_iat).seconds
        if seconds_since_issue > 60 * jwt_exp_mins:
            print('Refreshing token after {}s'.format(seconds_since_issue))
            jwt_iat = datetime.datetime.utcnow()
            client.loop()
            client.disconnect()
            client = get_client(
                args.project_id, args.cloud_region,
                args.registry_id, args.device_id, args.private_key_file,
                args.algorithm, args.ca_certs, args.mqtt_bridge_hostname,
                args.mqtt_bridge_port)
        # [END iot_mqtt_jwt_refresh]
        # Publish "payload" to the MQTT topic. qos=1 means at least once
        # delivery. Cloud IoT Core also supports qos=0 for at most once
        # delivery.
        client.publish(mqtt_topic, payload, qos=1)

        
        # Send events every second. State should not be updated as often
        time.sleep(1)
        # for i in range(0, 60):
        #     time.sleep(1)
        #     client.loop()
    # [END iot_mqtt_run]

def read_sensor(count):
    tempF = 20 + 0.2*count + (random.random() * 15)
    humidity = 60 + 0.3*count+ (random.random() * 20)
    temp = '{0:0.2f}'.format(tempF)
    hum = '{0:0.2f}'.format(humidity)
    sensorZipCode = 95192#"94043"
    sensorLat = 37.3382082+ (random.random() /100)#"37.421655"
    sensorLong = -121.8863286 + (random.random() /100)#"-122.085637"
    sensorLatf = '{0:0.6f}'.format(sensorLat)
    sensorLongf = '{0:0.6f}'.format(sensorLong)
    return (temp, hum, sensorZipCode, sensorLatf, sensorLongf)

def createJSON(reg_id, dev_id, timestamp, zip, lat, long, temperature, humidity, img_file):
    data = {
      'registry_id' : reg_id,
      'device_id' : dev_id,
      'timecollected' : timestamp,
      'zipcode' : zip,
      'latitude' : lat,
      'longitude' : long,
      'temperature' : temperature,
      'humidity' : humidity,
      'image_file' : img_file
    }

    json_str = json.dumps(data)
    return json_str

def storage_mqtt_device_demo(args):

    """Connects a device, sends data, and receives data."""
    # [START iot_mqtt_run]
    global minimum_backoff_time
    global MAXIMUM_BACKOFF_TIME

    # Publish to the events or state topic based on the flag.
    sub_topic = 'events' if args.message_type == 'event' else 'state'

    mqtt_topic = '/devices/{}/{}'.format(args.device_id, sub_topic)

    jwt_iat = datetime.datetime.utcnow()
    jwt_exp_mins = args.jwt_expires_minutes
    client = get_client(
        args.project_id, args.cloud_region, args.registry_id,
        args.device_id, args.private_key_file, args.algorithm,
        args.ca_certs, args.mqtt_bridge_hostname, args.mqtt_bridge_port)
    
    # Instantiates a google cloud client
    # Instantiates a client
    storage_client = storage.Client.from_service_account_json(args.service_account_json)
    #storage_client = storage.Client()
    #upload image to an existing bucket
    bucketexist = storage_client.bucket('cmpelkk_imagetest')

    i = 0
    path = args.imagefolder_path#'/Users/lkk/Documents/GoogleCloud/iotpython/data'
    for filename in os.listdir(path):
      if filename.endswith(".jpg"): 
        print(filename)#os.path.join(directory, filename))
        i+=1
        bucketfilename= "img%s.jpg" % i
        print(bucketfilename)
        blobexist = bucketexist.blob(bucketfilename)
        filepathlocal = os.path.join(path, filename)
        print(filepathlocal)
        blobexist.upload_from_filename(filepathlocal)

          # Process network events.
        client.loop()

        currentTime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        (temp, hum, sensorZipCode, sensorLat, sensorLong) = read_sensor(i)
        #(id, timestamp, zip, lat, long, temperature, humidity, img_file)
        payloadJSON = createJSON(args.registry_id, args.device_id, currentTime, sensorZipCode, sensorLat, sensorLong, temp, hum, bucketfilename)

        #payload = '{}/{}-image-{}'.format(args.registry_id, args.device_id, i)
        print('Publishing message {}/: \'{}\''.format(
            i, payloadJSON))

        # Publish "payload" to the MQTT topic. qos=1 means at least once
        # delivery. Cloud IoT Core also supports qos=0 for at most once
        # delivery.
        client.publish(mqtt_topic, payloadJSON, qos=1)

        
        # Send events every second. State should not be updated as often
        time.sleep(1)

        continue
      else:
        continue

def bigquery_mqtt_device_demo(args):

    """Connects a device, sends data, and receives data."""
    # [START iot_mqtt_run]
    global minimum_backoff_time
    global MAXIMUM_BACKOFF_TIME

    # Publish to the events or state topic based on the flag.
    sub_topic = 'events' if args.message_type == 'event' else 'state'

    mqtt_topic = '/devices/{}/{}'.format(args.device_id, sub_topic)

    jwt_iat = datetime.datetime.utcnow()
    jwt_exp_mins = args.jwt_expires_minutes
    client = get_client(
        args.project_id, args.cloud_region, args.registry_id,
        args.device_id, args.private_key_file, args.algorithm,
        args.ca_certs, args.mqtt_bridge_hostname, args.mqtt_bridge_port)
    
    # Instantiates a google cloud client
    # Instantiates a client
    #storage_client = storage.Client.from_service_account_json(args.service_account_json)
    #bucketexist = storage_client.bucket('cmpelkk_imagetest')

    i = 0
    for i in range(1, args.num_messages + 1):
        bucketfilename= "img%s.jpg" % i
        
        client.loop()

        currentTime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        (temp, hum, sensorZipCode, sensorLat, sensorLong) = read_sensor(i)
        #(id, timestamp, zip, lat, long, temperature, humidity, img_file)
        payloadJSON = createJSON(args.registry_id, args.device_id, currentTime, sensorZipCode, sensorLat, sensorLong, temp, hum, bucketfilename)

        #payload = '{}/{}-image-{}'.format(args.registry_id, args.device_id, i)
        print('Publishing message {}/: \'{}\''.format(
            i, payloadJSON))

        # Publish "payload" to the MQTT topic. qos=1 means at least once
        # delivery. Cloud IoT Core also supports qos=0 for at most once
        # delivery.
        client.publish(mqtt_topic, payloadJSON, qos=1)

        
        # Send events every second. State should not be updated as often
        time.sleep(0.5)

class Args:
  algorithm = 'RS256'
  ca_certs = '/Users/lkk/Documents/GoogleCloud/certs/roots.pem' #'/content/gdrive/"My Drive"/CurrentWork/CMPE181Sp2020/Googlecerts/roots.pem'
  cloud_region = 'us-central1'
  data = 'Hello there'
  device_id = 'cmpe181dev1'
  jwt_expires_minutes = 20
  listen_dur = 60
  message_type = 'event'
  mqtt_bridge_hostname = 'mqtt.googleapis.com'
  mqtt_bridge_port = 8883
  num_messages = 20000
  private_key_file = '/Users/lkk/Documents/GoogleCloud/certs/cmpe181dev1/rsa_private.pem'#/content/gdrive/"My Drive"/CurrentWork/CMPE181Sp2020/Googlecerts/cmpe181dev1/rsa_private.pem'
  project_id = 'cmpelkk'
  registry_id = 'CMPEIoT1'
  service_account_json = '/Users/lkk/Documents/GoogleCloud/certs/cmpelkk-134cc577da4f.json'#os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
  imagefolder_path= '/Users/lkk/Documents/GoogleCloud/iotpython/data'


def main():
    #args = parse_command_line_args()
    #mqtt_device_demo(args)
    args=Args()
    #cloudstorage_demo(args)
    #storage_mqtt_device_demo(args)
    bigquery_mqtt_device_demo(args)
    print('Finished.')


if __name__ == '__main__':
    main()