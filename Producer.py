import oci
import json
from base64 import b64encode

ociMessageEndpoint = "https://cell-1.streaming.ap-seoul-1.oci.oraclecloud.com"
ociStreamOcid = "ocid1.stream.oc1.ap-seoul-1.amaaaaaakas5ezqalmgeg65gsloe2ommmguix3vgj3xh32hj5p7s6wc6o3aa"
ociConfigFilePath = "~/.oci/config"  
ociProfileName = "DEFAULT"  


def produce_messages(client, stream_id):
    # Build up a PutMessagesDetails and publish some messages to the stream
    message_list = []
    with open("C:\\Users\\pelei\\Desktop\\demo-testing-data.json",'r') as load_f:
        load_dict = json.load(load_f)  

    for kv in load_dict['data']:
        key = str(kv['timestamp'])
        value = str(kv['values'])
        encoded_key = b64encode(key.encode()).decode()
        encoded_value = b64encode(value.encode()).decode()
        message_list.append(oci.streaming.models.PutMessagesDetailsEntry(key=encoded_key, value=encoded_value))

    print("Publishing {} messages to the stream {} ".format(len(message_list), stream_id))
    messages = oci.streaming.models.PutMessagesDetails(messages=message_list)
    put_message_result = client.put_messages(stream_id, messages)

    # The put_message_result can contain some useful metadata for handling failures
    for entry in put_message_result.data.entries:
        if entry.error:
            print("Error ({}) : {}".format(entry.error, entry.error_message))
        else:
            print("Published message to partition {} , offset {}".format(entry.partition, entry.offset))


config = oci.config.from_file(ociConfigFilePath, ociProfileName)
# config = oci.config.from_file()

stream_client = oci.streaming.StreamClient(config, service_endpoint=ociMessageEndpoint)

# Publish some messages to the stream
produce_messages(stream_client, ociStreamOcid)