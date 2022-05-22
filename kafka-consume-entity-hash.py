#pip install kafka-python psycopg2-binary

from kafka import KafkaConsumer
import json
import psycopg2
import hashlib
import time

print("imported libs")

'''
-- Link clicks count
select
    jsonb_extract_path_text(a.action_attributes, 'url_domain_name') as url_domain_name,
    es.entity_id as subject_id,
    --jsonb_extract_path_text(a.action_attributes, 'url') as url,
    count(*) as click_cnt
from action_key_website_link a
    left join entity_key_website_link es
        on es.entity_key = a.subject_key
where jsonb_extract_path_text(a.action_attributes, 'url_domain_name') = 'data-for.me'
group by 1,2
order by 3 desc
;


-- Page views count
select
    jsonb_extract_path_text(a.action_attributes, 'url_domain_name') as url_domain_name,
    es.entity_id as subject_id,
    --jsonb_extract_path_text(a.action_attributes, 'url') as url,
    count(*) as view_cnt,
    count(distinct a.proxy_origin) as origin_cnt
from action_key_website_page a
    left join entity_key_website_page es
        on es.entity_key = a.subject_key
where jsonb_extract_path_text(a.action_attributes, 'url_domain_name') = 'data-for.me'
group by 1,2
order by 3 desc
;


-- Website session durations
select
    url_domain_name,
    event_date,
    avg(duration_min) as duration_min
from (
    select
        date_trunc('month', a.event_timestamp)::date as event_date,
        jsonb_extract_path_text(a.action_attributes, 'url_domain_name') as url_domain_name,
        jsonb_extract_path_text(a.action_attributes, 'session_id') as session_id,
        count(*) as event_cnt,
        extract(epoch from max(a.event_timestamp) - min(a.event_timestamp))/60 as duration_min
    from action_key_website_page a
    where jsonb_extract_path_text(a.action_attributes, 'url_domain_name') is not null
        and a.event_timestamp >= '2019-01-01'
    group by 1,2,3
    order by 4 desc
) t
--where event_cnt > 1
group by 1,2
order by 1,2
;


-- Device session durations
select
    event_date,
    avg(duration_min) as duration_min
from (
    select
        date_trunc('month', a.event_timestamp)::date as event_date,
        jsonb_extract_path_text(a.action_attributes, 'session_id') as session_id,
        count(*) as event_cnt,
        extract(epoch from max(a.event_timestamp) - min(a.event_timestamp))/60 as duration_min
    from action_key_device a
    where a.event_timestamp >= '2019-01-01'
    group by 1,2
    order by 3 desc
) t
--where event_cnt > 1
group by 1
order by 1
;


-- Recent android device events
select
    eo.entity_id as origin_id,
    jsonb_extract_path_text(a.action_attributes, 'session_id') as session_id,
    a.event_id,
    a.event_timestamp,
    a.action_type,
    jsonb_extract_path_text(a.action_attributes, 'battery_level') as battery_level
from public.action_key_device a
    left join public.entity_key_android_device eo
        on eo.entity_key = a.origin_key
order by a.event_timestamp desc
limit 100
;


-- Recent website link clicks
select
    ea.entity_id as actor_id,
    jsonb_extract_path_text(a.action_attributes, 'session_id') as session_id,
    a.event_id,
    a.event_timestamp,
    a.action_type,
    es.entity_id as subject_id,
    jsonb_extract_path_text(a.action_attributes, 'url') as url,
    jsonb_extract_path_text(a.action_attributes, 'url_referral') as url_referral,
    jsonb_extract_path_text(a.action_attributes, 'device_type') as device_type,
    jsonb_extract_path_text(a.action_attributes, 'app_platform') as app_platform
from public.action_key_website_link a
    left join public.entity_key_website_link es
        on es.entity_key = a.subject_key
    left join public.entity_key_website_visitor ea
        on ea.entity_key = a.actor_key
order by a.event_timestamp desc
limit 100
;


-- Recent website page views
select
    ea.entity_id as actor_id,
    jsonb_extract_path_text(a.action_attributes, 'session_id') as session_id,
    a.event_id,
    a.event_timestamp,
    a.action_type,
    es.entity_id as subject_id,
    jsonb_extract_path_text(a.action_attributes, 'url_referral') as url_referral,
    jsonb_extract_path_text(a.action_attributes, 'device_type') as device_type,
    jsonb_extract_path_text(a.action_attributes, 'app_platform') as app_platform
from public.action_key_website_page a
    left join public.entity_key_website_page es
        on es.entity_key = a.subject_key
    left join public.entity_key_website_visitor ea
        on ea.entity_key = a.actor_key
order by a.event_timestamp desc
limit 100
;



-- Page views monthly count
select
    date_trunc('month', a.event_timestamp)::date as event_date,
    jsonb_extract_path_text(a.action_attributes, 'url_domain_name') as url_domain_name,
    count(*) as view_cnt
from action_key_website_page a
where a.event_timestamp >= '2019-01-01'
    and jsonb_extract_path_text(a.action_attributes, 'url_domain_name') is not null
group by 1,2
order by 1,2
;


-- Device events monthly count
select
    date_trunc('month', a.event_timestamp)::date as event_date,
    a.action_type,
    count(*) as event_cnt
from action_key_device a
where a.event_timestamp >= '2019-01-01'
    and a.action_type != 'battery_changed'
group by 1,2
order by 1,2
;


-- Device events daily count
select
    date_trunc('day', a.event_timestamp)::date as event_date,
    a.action_type,
    count(*) as event_cnt
from action_key_device a
where a.event_timestamp >= current_date - 90
    and a.action_type != 'battery_changed'
group by 1,2
order by 1,2
;


-- Device battery level hourly
select
    date_trunc('hour', a.event_timestamp) as event_date,
    min(jsonb_extract_path_text(a.action_attributes, 'battery_level')::int) as min_battery_level
from action_key_device a
where a.event_timestamp >= current_date - 9
    and a.action_type = 'battery_changed'
group by 1
order by 1
;

'''

entity_table_ddl = """
create table if not exists entity_key_{type} (
    entity_key varchar(64) primary key,
    entity_id text,
    date_created timestamp,
    last_updated timestamp,
    attributes jsonb
);
"""

action_table_ddl = """
create table if not exists action_key_{type} (
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
create index if not exists action_key_{type}_event_id on action_key_{type} (event_id);
create index if not exists action_key_{type}_origin_key on action_key_{type} (origin_key);
create index if not exists action_key_{type}_actor_key on action_key_{type} (actor_key);
create index if not exists action_key_{type}_subject_key on action_key_{type} (subject_key);
"""

entity_table_update_sql = """
update entity_key_{type}
set
    attributes = coalesce(attributes, '{{}}') || %(entity_attributes)s,
    last_updated = %(event_timestamp)s
where entity_key = %(entity_key)s
   and coalesce(%(entity_attributes)s, '') != ''
;
"""

entity_table_insert_sql = """
insert into entity_key_{type} (
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
    select 1 from entity_key_{type}
    where entity_key = %(entity_key)s
);
"""

action_table_insert_sql = """
insert into action_key_{type} (
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
    select 1 from action_key_{type}
    where event_id = %(event_id)s
);
"""

print("creating KafkaConsumer")

consumer = KafkaConsumer(
    bootstrap_servers=['input.data-for.me:9092'],
    auto_offset_reset='earliest',
    #group_id=None
    enable_auto_commit=True,
    group_id='test-python-consumer-entity-hash'
)

print("subscribing KafkaConsumer")

consumer.subscribe(pattern='^events_.*')

current_milli_time = lambda: int(round(time.time() * 1000))

time_from = current_milli_time()

print("connecting psycopg2")

conn = psycopg2.connect(host='input.data-for.me', port='5432', dbname='dp', user='dp', password='dp')

known_entities = set([])
known_entity_hashes = set([])

print("reading events from KafkaConsumer")

try:
    for message in consumer:
        time_start = current_milli_time()

        if (message.value and message.value.decode('UTF-8')[0] == "{"):

            print ("%s:%d:%d: key=%s" % (message.topic, message.partition,
                                         message.offset, message.key))
            event_json = json.loads(message.value.decode('UTF-8'))
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

                #print('Generating hash for origin = ' + '^'.join(filter(None, (event_json["origin"]["type"], event_json["origin"]["id"]))))
                origin_key = hashlib.sha256('^'.join(filter(None, (event_json["origin"]["type"], event_json["origin"]["id"]))).encode('UTF-8')).hexdigest()
                #print('Generating hash for actor = ' + '^'.join(filter(None, (event_json["actor"]["type"], event_json["actor"]["id"]))))
                actor_key = hashlib.sha256('^'.join(filter(None, (event_json["actor"]["type"], event_json["actor"]["id"]))).encode('UTF-8')).hexdigest()
                #print('Generating hash for subject = ' + '^'.join(filter(None, (event_json["subject"]["type"], event_json["subject"]["id"]))))
                subject_key = hashlib.sha256('^'.join(filter(None, (event_json["subject"]["type"], event_json["subject"]["id"]))).encode('UTF-8')).hexdigest()

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

        else:
            print ("Not a JSON event!")
            if message.value:
                print(message.value)

        print ("Total message time: " + str(current_milli_time() - time_start))

except KeyboardInterrupt:
    print ("User stop requested")
except:
    print("Unexpected error: ", sys.exc_info()[0])
    raise

finally:
    print ("Committing offsets")
    consumer.commit()
    print ("Closing connection")
    conn.commit()
    conn.close()
