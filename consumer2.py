import oci
import time
import datetime
import mysql.connector
import ast

from base64 import b64decode

ociMessageEndpoint = "https://cell-1.streaming.ap-seoul-1.oci.oraclecloud.com"  
ociStreamOcid = "ocid1.stream.oc1.ap-seoul-1.amaaaaaakas5ezqalmgeg65gsloe2ommmguix3vgj3xh32hj5p7s6wc6o3aa"  
ociConfigFilePath = "~/.oci/config"  
ociProfileName = "DEFAULT"

def insert_table(cnx, cursor, key_utc, value):

    add_salary = ("INSERT INTO streamtable "
    #               "(emp_no, salary, from_date, to_date) "
                  "VALUES (%(sdate)s, %(temperature_1)s, %(temperature_2)s, %(temperature_3)s, %(temperature_4)s, %(temperature_5)s,"
                  "%(pressure_1)s, %(pressure_2)s, %(pressure_3)s, %(pressure_4)s, %(pressure_5)s)")
    value_list = ast.literal_eval(value)
    
    UTC_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    utcTime = datetime.datetime.strptime(key_utc, UTC_FORMAT)
    localtime = utcTime + datetime.timedelta(hours=8)
    
    # Insert salary information
    data_salary = {
        'sdate': localtime,
        'temperature_1': value_list[0],
        'temperature_2': value_list[1],
        'temperature_3': value_list[2],
        'temperature_4': value_list[3],  
        'temperature_5': value_list[4],
        'pressure_1': value_list[5],
        'pressure_2': value_list[6],
        'pressure_3': value_list[7],
        'pressure_4': value_list[8],
        'pressure_5': value_list[9],
    }

    cursor.execute(add_salary, data_salary)

    # Make sure data is committed to the database
    cnx.commit()

def get_cursor_by_group(sc, sid, group_name, instance_name):
    print(" Creating a cursor for group {}, instance {}".format(group_name, instance_name))
    cursor_details = oci.streaming.models.CreateGroupCursorDetails(group_name=group_name, instance_name=instance_name,
                                                                   type=oci.streaming.models.
                                                                   CreateGroupCursorDetails.TYPE_TRIM_HORIZON,
                                                                   commit_on_get=True)
    response = sc.create_group_cursor(sid, cursor_details)
    return response.data.value

def simple_message_loop(client, stream_id, initial_cursor):
    cursor = initial_cursor
    
    cnx = mysql.connector.connect(user='root', password='Lp123#',
                              host='129.154.193.59',
                              database='streamdb')
    cursor_db = cnx.cursor()
    
    while True:
        get_response = client.get_messages(stream_id, cursor, limit=10)
        # No messages to process. return.
        if not get_response.data:
            return

        # Process the messages
        print(" Read {} messages".format(len(get_response.data)))
        for message in get_response.data:
            if message.key is None:
                key = "Null"
            else:
                key = b64decode(message.key.encode()).decode()
                insert_table(cnx, cursor_db, key, b64decode(message.value.encode()).decode())
            print("success insert {}: {}".format(key, b64decode(message.value.encode()).decode()))

        # get_messages is a throttled method; clients should retrieve sufficiently large message
        # batches, as to avoid too many http requests.
        time.sleep(1)
        # use the next-cursor for iteration
        cursor = get_response.headers["opc-next-cursor"]
    
    cursor_db.close()
    cnx.close()

config = oci.config.from_file(ociConfigFilePath, ociProfileName)
stream_client = oci.streaming.StreamClient(config, service_endpoint=ociMessageEndpoint)

# A cursor can be created as part of a consumer group.
# Committed offsets are managed for the group, and partitions
# are dynamically balanced amongst consumers in the group.
group_cursor = get_cursor_by_group(stream_client, ociStreamOcid, "example-group", "example-instance-1")
simple_message_loop(stream_client, ociStreamOcid, group_cursor)