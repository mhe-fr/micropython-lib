import ussl
import time
import ntptime
import ucrypto
import ubinascii
from simple import MQTTClient

DISCONNECTED = 0
CONNECTING = 1
CONNECTED = 2
GOOGLEMQTTHOST = "mqtt.googleapis.com"

class GCPIOT:

    def __init__(self, project_id, cloud_region, registry_id, device_id, keyfile, certfile):
        self.project_id = project_id
        self.cloud_region = cloud_region
        self.registry_id = registry_id
        self.device_id = device_id
        with open(keyfile, 'r') as f:
            self.keyData = f.read()
        with open(certfile, 'r') as f:
            self.certData = f.read()
        self.connection = None
        self.state = DISCONNECTED

    def connect(self):

        while self.state != CONNECTED:
            timestamp = ntptime.time() + 946684800 #01/01/1970 to 01/01/2000 = 10957 days * (24 * 3600) sec/days
            clientString = b'projects/'+ self.project_id + b'/locations/' + self.cloud_region + b'/registries/' + self.registry_id + b'/devices/' + self.device_id
            jwtheader = b'{"alg": "RS256", "typ": "JWT"}'
            jwtpayload = b'{"aud": "' + self.project_id + b'", "iat": ' + str(timestamp) + b', "exp": ' + str(timestamp + (24 * 3600))+ b'}'
            message = ubinascii.b2a_base64(jwtheader)[:-1] + b'.' + ubinascii.b2a_base64(jwtpayload)[:-1]
            jwt = message + b'.' + ubinascii.b2a_base64(ucrypto.rsa_sign(self.keyData, message))[:-1]
            print(jwt)
            try:
                self.state = CONNECTING
                self.connection = MQTTClient(client_id=clientString, user="johndoe", password=jwt, server=GOOGLEMQTTHOST, port=8883, keepalive=10000, ssl=True, ssl_params={"cert":self.certData, "key":self.keyData})
                self.connection.connect()
                self.state = CONNECTED
            except:
                print('Could not establish MQTT connection')
                time.sleep(0.5)
                raise
                continue

        print('MQTT LIVE!')

    def publish(self, topic, msg):
        if self.state == CONNECTED:
            return self.connection.publish(topic, msg)

    def disconnect(self):
        self.connection.disconnect()        
        self.state = DISCONNECTED
