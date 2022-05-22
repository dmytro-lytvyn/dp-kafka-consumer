from kafka import KafkaConsumer
import json
import psycopg2
import time

entity_table_ddl = """
create table if not exists entity_{type} (
    id text primary key,
    date_created timestamp,
    last_updated timestamp,
    attributes jsonb
);
"""

action_table_ddl = """
create table if not exists action_{type} (
    event_id text,
    event_timestamp timestamp,
    origin_type text,
    origin_id text,
    actor_type text,
    actor_id text,
    subject_id text,
    action_type text,
    action_attributes jsonb
);
create index if not exists action_{type}_event_id on action_{type} (event_id);
create index if not exists action_{type}_origin_id on action_{type} (origin_id);
create index if not exists action_{type}_actor_id on action_{type} (actor_id);
create index if not exists action_{type}_subject_id on action_{type} (subject_id);
"""

entity_table_update_sql = """
update entity_{type}
set
    attributes = attributes || %(entity_attributes)s,
    last_updated = %(event_timestamp)s
where id = %(entity_id)s
    and coalesce(%(entity_attributes)s, '') != ''
;
"""

entity_table_insert_sql = """
insert into entity_{type} (
    id,
    date_created,
    last_updated,
    attributes
)
select
    %(entity_id)s,
    %(event_timestamp)s,
    %(event_timestamp)s,
    %(entity_attributes)s
where not exists (
    select 1 from entity_{type}
    where id = %(entity_id)s
);
"""

action_table_insert_sql = """
insert into action_{type} (
    event_id,
    event_timestamp,
    origin_type,
    origin_id,
    actor_type,
    actor_id,
    subject_id,
    action_type,
    action_attributes
)
select
    %(event_id)s,
    %(event_timestamp)s,
    %(origin_type)s,
    %(origin_id)s,
    %(actor_type)s,
    %(actor_id)s,
    %(subject_id)s,
    %(action_type)s,
    %(action_attributes)s
where not exists (
    select 1 from action_{type}
    where event_id = %(event_id)s
);
"""

consumer = KafkaConsumer(
    bootstrap_servers=['input.data-for.me:9092'],
    auto_offset_reset='earliest',
    #group_id=None
    enable_auto_commit=False,
    group_id='test-python-consumer'
)

consumer.subscribe(pattern='^events_.*')

current_milli_time = lambda: int(round(time.time() * 1000))

time_from = current_milli_time()

conn = psycopg2.connect(host='input.data-for.me', port='5432', dbname='dp', user='dp', password='dp')

try:
    for message in consumer:

        print ("Time 1: ", (current_milli_time() - time_from))
        time_from = current_milli_time()
        time_start = time_from

        if (message.value and message.value[0] == "{"):

            print ("Time 2: ", (current_milli_time() - time_from))
            time_from = current_milli_time()

            print ("%s:%d:%d: key=%s" % (message.topic, message.partition,
                                         message.offset, message.key))
            event_json = json.loads(message.value.decode('utf-8'))
            print (event_json)

            print ("Time 3: ", (current_milli_time() - time_from))
            time_from = current_milli_time()

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

                print ("Time 4: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                print ("Time 5: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                cur = conn.cursor()

                print ("Time 6.1: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                cur.execute(entity_table_ddl.format(type=event_json["origin"]["type"]))

                print ("Time 6.2: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                cur.execute(entity_table_ddl.format(type=event_json["actor"]["type"]))

                print ("Time 6.3: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                cur.execute(entity_table_ddl.format(type=event_json["subject"]["type"]))

                print ("Time 6.4: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                cur.execute(action_table_ddl.format(type=event_json["subject"]["type"]))
                cur.close()

                print ("Time 7: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                conn.commit()

                print ("Time 8: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                cur = conn.cursor()
                entity_attributes = None
                print ("entity_table_insert_sql - origin")
                cur.execute(entity_table_insert_sql.format(type=event_json["origin"]["type"]),
                    {
                        'entity_id': event_json["origin"]["id"],
                        'event_timestamp': event_json["event_timestamp"],
                        'entity_attributes': entity_attributes
                    }
                )

                print ("Time 8.1: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                entity_attributes = None
                print ("entity_table_insert_sql - actor")
                cur.execute(entity_table_insert_sql.format(type=event_json["actor"]["type"]),
                    {
                        'entity_id': event_json["actor"]["id"],
                        'event_timestamp': event_json["event_timestamp"],
                        'entity_attributes': entity_attributes
                    }
                )

                print ("Time 8.2: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                if "attributes" in event_json["subject"]:
                    entity_attributes = json.dumps(event_json["subject"]["attributes"])
                else:
                    entity_attributes = None
                print ("entity_table_update_sql - subject")
                cur.execute(entity_table_update_sql.format(type=event_json["subject"]["type"]),
                    {
                        'entity_id': event_json["subject"]["id"],
                        'event_timestamp': event_json["event_timestamp"],
                        'entity_attributes': entity_attributes
                    }
                )
                
                print ("Time 8.3: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                print ("entity_table_insert_sql - subject")
                cur.execute(entity_table_insert_sql.format(type=event_json["subject"]["type"]),
                    {
                        'entity_id': event_json["subject"]["id"],
                        'event_timestamp': event_json["event_timestamp"],
                        'entity_attributes': entity_attributes
                    }
                )

                print ("Time 8.4: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                if "attributes" in event_json["action"]:
                    action_attributes = json.dumps(event_json["action"]["attributes"])
                else:
                    action_attributes = None
                print ("action_table_insert_sql - subject")
                cur.execute(action_table_insert_sql.format(type=event_json["subject"]["type"]),
                    {
                        'event_id': event_json["event_id"],
                        'event_timestamp': event_json["event_timestamp"],
                        'origin_type': event_json["origin"]["type"],
                        'origin_id': event_json["origin"]["id"],
                        'actor_type': event_json["actor"]["type"],
                        'actor_id': event_json["actor"]["id"],
                        'subject_id': event_json["subject"]["id"],
                        'action_type': event_json["action"]["type"],
                        'action_attributes': action_attributes
                    }
                )
                cur.close()

                print ("Time 9: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

                conn.commit()

                print ("Time 10: ", (current_milli_time() - time_from))
                time_from = current_milli_time()

        consumer.commit() # Commit offset for each loop

        print ("Time 11: ", (current_milli_time() - time_from))
        time_from = current_milli_time()
        print ("Total message time: ", (time_from - time_start))

except KeyboardInterrupt:
    print ("User stop requested")

finally:
    print ("Committing offsets")
    consumer.commit()
    print ("Closing connection")
    conn.close()
