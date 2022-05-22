#pip install confluent-kafka
#pip install setuptools
#pip install psycopg2-binary

from confluent_kafka import Consumer, KafkaError
import json
import psycopg2
import hashlib
import time

entity_table_ddl = """
create table if not exists entity_confluent_{type} (
    entity_key varchar(64) primary key,
    entity_id text,
    date_created timestamp,
    last_updated timestamp,
    attributes jsonb
);
"""

action_table_ddl = """
create table if not exists action_confluent_{type} (
    event_id char(36),
    event_timestamp timestamp,
    proxy_timestamp timestamp,
    proxy_origin varchar(64),
    subject_key char(64),
    origin_type text,
    origin_key char(64),
    actor_type text,
    actor_key char(64),
    action_type text,
    action_attributes jsonb
);
create index if not exists action_confluent_{type}_event_id on action_confluent_{type} (event_id);
create index if not exists action_confluent_{type}_origin_key on action_confluent_{type} (origin_key);
create index if not exists action_confluent_{type}_actor_key on action_confluent_{type} (actor_key);
create index if not exists action_confluent_{type}_subject_key on action_confluent_{type} (subject_key);
"""

entity_table_update_sql = """
update entity_confluent_{type}
set
    attributes = coalesce(attributes, '{{}}') || %(entity_attributes)s,
    last_updated = %(event_timestamp)s
where entity_key = %(entity_key)s
   and coalesce(%(entity_attributes)s, '') != ''
;
"""

entity_table_insert_sql = """
insert into entity_confluent_{type} (
    entity_key,
    entity_id,
    date_created,
    last_updated,
    attributes
)
select
    %(entity_key)s,
    %(entity_id)s,
    %(event_timestamp)s,
    %(event_timestamp)s,
    %(entity_attributes)s
where not exists (
    select 1 from entity_confluent_{type}
    where entity_key = %(entity_key)s
);
"""

action_table_insert_sql = """
insert into action_confluent_{type} (
    event_id,
    event_timestamp,
    proxy_timestamp,
    proxy_origin,
    subject_key,
    origin_type,
    origin_key,
    actor_type,
    actor_key,
    action_type,
    action_attributes
)
select
    %(event_id)s,
    %(event_timestamp)s,
    %(proxy_timestamp)s,
    %(proxy_origin)s,
    %(subject_key)s,
    %(origin_type)s,
    %(origin_key)s,
    %(actor_type)s,
    %(actor_key)s,
    %(action_type)s,
    %(action_attributes)s
where not exists (
    select 1 from action_confluent_{type}
    where event_id = %(event_id)s
);
"""

consumer = Consumer({
    'bootstrap.servers': 'input.data-for.me',
    'group.id': 'test-python-consumer-confluent-v2',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['^events_.*'])

current_milli_time = lambda: int(round(time.time() * 1000))

time_from = current_milli_time()

conn = psycopg2.connect(host='input.data-for.me', port='5432', dbname='dp', user='dp', password='dp')

known_entities = set([])
known_entity_hashes = set([])

try:
    while True:
        time_start = current_milli_time()

        message = consumer.poll()

        if message is None:
            print("Empty message")
            continue

        if message.error():
            print("Consumer error: {}".format(message.error()))
            continue

        print ("%s:%d:%d: key=%s" % (message.topic(), message.partition(),
                                     message.offset(), message.key()))
        event_json = json.loads(message.value().decode('utf-8'))
        print (event_json)

        if ("event_id" in event_json
            and "event_timestamp" in event_json
            and "origin" in event_json
            and "type" in event_json["origin"]
            and "id" in event_json["origin"]
            and "actor" in event_json
            and "type" in event_json["actor"]
            and "id" in event_json["actor"]
            and "subject" in event_json
            and "type" in event_json["subject"]
            and "id" in event_json["subject"]
            and "action" in event_json
            and "type" in event_json["action"]):

            print ("Creating tables start...")
            time_from = current_milli_time()

            # Creating tables
            is_origin_known = event_json["origin"]["type"] in known_entities
            is_actor_known = event_json["actor"]["type"] in known_entities
            is_subject_known = event_json["subject"]["type"] in known_entities

            if not (is_origin_known and is_actor_known and is_subject_known):
                cur = conn.cursor()

                if not is_origin_known:
                    print ("Creating a table for origin entity")
                    cur.execute(entity_table_ddl.format(type=event_json["origin"]["type"]))
                    known_entities.add(event_json["origin"]["type"])
                else:
                    print ("Origin entity " + event_json["origin"]["type"] + " is already known, skipping table create")

                if not is_actor_known:
                    print ("Creating a table for actor entity")
                    cur.execute(entity_table_ddl.format(type=event_json["actor"]["type"]))
                    known_entities.add(event_json["actor"]["type"])
                else:
                    print ("Actor entity " + event_json["origin"]["type"] + " is already known, skipping table create")

                if not is_subject_known:
                    print ("Creating tables for subject entity and actions")
                    cur.execute(entity_table_ddl.format(type=event_json["subject"]["type"]))
                    cur.execute(action_table_ddl.format(type=event_json["subject"]["type"]))
                    known_entities.add(event_json["subject"]["type"])
                else:
                    print ("Subject entity " + event_json["origin"]["type"] + " is already known, skipping table create")

                cur.close()
                conn.commit()

            print ("Creating tables end: " + str(current_milli_time() - time_from))

            # Inserting entities and action
            print ("Inserting records start...")
            time_from = current_milli_time()

            cur = conn.cursor()

            origin_key = hashlib.sha256('^'.join(filter(None, (event_json["origin"]["type"].encode('utf-8'), event_json["origin"]["id"].encode('utf-8'))))).hexdigest()
            actor_key = hashlib.sha256('^'.join(filter(None, (event_json["actor"]["type"].encode('utf-8'), event_json["actor"]["id"].encode('utf-8'))))).hexdigest()
            subject_key = hashlib.sha256('^'.join(filter(None, (event_json["subject"]["type"].encode('utf-8'), event_json["subject"]["id"].encode('utf-8'))))).hexdigest()

            print ("Generating key hashes done: " + str(current_milli_time() - time_from))

            is_origin_hash_known = origin_key in known_entity_hashes
            is_actor_hash_known = actor_key in known_entity_hashes
            is_subject_hash_known = subject_key in known_entity_hashes
            is_subject_has_attributes = "attributes" in event_json["subject"]

            if not is_origin_hash_known:
                entity_attributes = None
                print ("Inserting origin entity...")
                cur.execute(entity_table_insert_sql.format(type=event_json["origin"]["type"]),
                    {
                        'entity_key': origin_key,
                        'entity_id': event_json["origin"]["id"],
                        'event_timestamp': event_json["event_timestamp"],
                        'entity_attributes': entity_attributes
                    }
                )
                known_entity_hashes.add(origin_key)
            else:
                print ("Origin entity key " + origin_key + " is already known, skipping insert entity")

            if not is_actor_hash_known:
                entity_attributes = None
                print ("Inserting actor entity...")
                cur.execute(entity_table_insert_sql.format(type=event_json["actor"]["type"]),
                    {
                        'entity_key': actor_key,
                        'entity_id': event_json["actor"]["id"],
                        'event_timestamp': event_json["event_timestamp"],
                        'entity_attributes': entity_attributes
                    }
                )
                known_entity_hashes.add(actor_key)
            else:
                print ("Actor entity key " + actor_key + " is already known, skipping insert entity")

            if is_subject_has_attributes:
                entity_attributes = json.dumps(event_json["subject"]["attributes"])

                print ("Updating subject entity if exists")
                cur.execute(entity_table_update_sql.format(type=event_json["subject"]["type"]),
                    {
                        'entity_key': subject_key,
                        'entity_id': event_json["subject"]["id"],
                        'event_timestamp': event_json["event_timestamp"],
                        'entity_attributes': entity_attributes
                    }
                )
                known_entity_hashes.add(subject_key)
            else:
                print ("Subject entity doesn't have attributes, skipping update entity")

            if not is_subject_hash_known:
                print ("Inserting subject entity...")
                cur.execute(entity_table_insert_sql.format(type=event_json["subject"]["type"]),
                    {
                        'entity_key': subject_key,
                        'entity_id': event_json["subject"]["id"],
                        'event_timestamp': event_json["event_timestamp"],
                        'entity_attributes': entity_attributes
                    }
                )
                known_entity_hashes.add(subject_key)
            else:
                print ("Subject entity key " + subject_key + " is already known, skipping insert entity")

            if "_proxy_timestamp" in event_json:
                proxy_timestamp = event_json["_proxy_timestamp"]
            else:
                proxy_timestamp = None

            if "_proxy_origin" in event_json:
                proxy_origin = event_json["_proxy_origin"]
            else:
                proxy_origin = None

            if "attributes" in event_json["action"]:
                action_attributes = json.dumps(event_json["action"]["attributes"])
            else:
                action_attributes = None

            print ("Inserting subject action...")
            cur.execute(action_table_insert_sql.format(type=event_json["subject"]["type"]),
                {
                    'event_id': event_json["event_id"],
                    'event_timestamp': event_json["event_timestamp"],
                    'proxy_timestamp': proxy_timestamp,
                    'proxy_origin': proxy_origin,
                    'subject_key': subject_key,
                    'origin_type': event_json["origin"]["type"],
                    'origin_key': origin_key,
                    'actor_type': event_json["actor"]["type"],
                    'actor_key': actor_key,
                    'action_type': event_json["action"]["type"],
                    'action_attributes': action_attributes
                }
            )

            cur.close()
            conn.commit()

            print ("Inserting records end: " + str(current_milli_time() - time_from))
        else:
            print ("Not all required event fields are available")

        print ("Total message time: " + str(current_milli_time() - time_start))

except KeyboardInterrupt:
    print ("User stop requested")

finally:
    print ("Closing consumer")
    consumer.close()
    print ("Closing connection")
    conn.commit()
    conn.close()
