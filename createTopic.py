from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json


# Read arguments and configurations and initialize
admin_client_conf = json.load(open('cred.json'))
topic= "smartMeter"
num_partitions=6
replication_factor=3

a = AdminClient(admin_client_conf)

fs = a.create_topics(
    [NewTopic(topic,num_partitions=num_partitions,replication_factor=replication_factor)]
    )
    
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
        # Otherwise fail fast
        if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            print("Failed to create topic {}: {}".format(topic, e))