from kafka import KafkaConsumer
import json
import psycopg2


entity_table_ddl = """
create table if not exists entity (
  id text,
  type text,
  date_created timestamp,
  last_updated timestamp,
  attributes jsonb,
  primary key (id, type)
);
"""

action_table_ddl = """
create table if not exists action (
  event_id text,
  event_timestamp timestamp,
  origin_type text,
  origin_id text,
  actor_type text,
  actor_id text,
  subject_type text,
  subject_id text,
  action_type text,
  action_attributes jsonb
);
create index if not exists action_event_id on action (event_id);
create index if not exists action_origin on action (origin_type, origin_id);
create index if not exists action_actor on action (actor_type, actor_id);
create index if not exists action_subject on action (subject_type, subject_id);
"""

entity_table_update_sql = """
update entity
set
  attributes = attributes || %(entity_attributes)s,
  last_updated = %(event_timestamp)s
where id = %(entity_id)s
  and type = %(entity_type)s
  and coalesce(%(entity_attributes)s, '') != ''
;
"""

entity_table_insert_sql = """
insert into entity (
  id,
  type,
  date_created,
  last_updated,
  attributes
)
select
  %(entity_id)s,
  %(entity_type)s,
  %(event_timestamp)s,
  %(event_timestamp)s,
  %(entity_attributes)s
where not exists (
  select 1 from entity
  where id = %(entity_id)s
    and type = %(entity_type)s
);
"""

action_table_insert_sql = """
insert into action (
  event_id,
  event_timestamp,
  origin_type,
  origin_id,
  actor_type,
  actor_id,
  subject_type,
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
  %(subject_type)s,
  %(subject_id)s,
  %(action_type)s,
  %(action_attributes)s
where not exists (
  select 1 from action
  where event_id = %(event_id)s
);
"""

consumer = KafkaConsumer(
    bootstrap_servers=['input.data-for.me:9092'],
    auto_offset_reset='earliest',
    #group_id=None
    enable_auto_commit=True,
    group_id='test-python-consumer-single-table'
)

consumer.subscribe(pattern='^events_.*')

for message in consumer:
    if (message.value and message.value[0] == "{"):
        print ("%s:%d:%d: key=%s" % (message.topic, message.partition,
                                     message.offset, message.key))
        event_json = json.loads(message.value.decode('utf-8'))
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

            conn = psycopg2.connect(host='input.data-for.me', port='5432', dbname='dp', user='dp', password='dp')

            cur = conn.cursor()
            cur.execute(entity_table_ddl)
            cur.execute(entity_table_ddl)
            cur.execute(entity_table_ddl)
            cur.execute(action_table_ddl)
            cur.close()

            conn.commit()

            cur = conn.cursor()
            entity_attributes = None
            print ("entity_table_insert_sql - origin")
            cur.execute(entity_table_insert_sql,
                {
                    'entity_id': event_json["origin"]["id"],
                    'entity_type': event_json["origin"]["type"],
                    'event_timestamp': event_json["event_timestamp"],
                    'entity_attributes': entity_attributes
                }
            )
            entity_attributes = None
            print ("entity_table_insert_sql - actor")
            cur.execute(entity_table_insert_sql,
                {
                    'entity_id': event_json["actor"]["id"],
                    'entity_type': event_json["actor"]["type"],
                    'event_timestamp': event_json["event_timestamp"],
                    'entity_attributes': entity_attributes
                }
            )
            if "attributes" in event_json["subject"]:
                entity_attributes = json.dumps(event_json["subject"]["attributes"])
            else:
                entity_attributes = None
            print ("entity_table_update_sql - subject")
            cur.execute(entity_table_update_sql,
                {
                    'entity_id': event_json["subject"]["id"],
                    'entity_type': event_json["subject"]["type"],
                    'event_timestamp': event_json["event_timestamp"],
                    'entity_attributes': entity_attributes
                }
            )
            print ("entity_table_insert_sql - subject")
            cur.execute(entity_table_insert_sql,
                {
                    'entity_id': event_json["subject"]["id"],
                    'entity_type': event_json["subject"]["type"],
                    'event_timestamp': event_json["event_timestamp"],
                    'entity_attributes': entity_attributes
                }
            )
            if "attributes" in event_json["action"]:
                action_attributes = json.dumps(event_json["action"]["attributes"])
            else:
                action_attributes = None
            print ("action_table_insert_sql - subject")
            cur.execute(action_table_insert_sql,
                {
                    'event_id': event_json["event_id"],
                    'event_timestamp': event_json["event_timestamp"],
                    'origin_type': event_json["origin"]["type"],
                    'origin_id': event_json["origin"]["id"],
                    'actor_type': event_json["actor"]["type"],
                    'actor_id': event_json["actor"]["id"],
                    'subject_type': event_json["subject"]["type"],
                    'subject_id': event_json["subject"]["id"],
                    'action_type': event_json["action"]["type"],
                    'action_attributes': action_attributes
                }
            )
            cur.close()

            conn.commit()

            conn.close()
