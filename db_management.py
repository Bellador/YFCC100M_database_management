import psycopg2
import csv
import sys
import json
import time
import pickle
import gc
import pandas as pd
from collections import Counter
import urllib.request
import urllib
import ssl

path_DATASET = "C:/Users/mhartman/Documents/100mDataset/yfcc100m_dataset"
path_AUTOTAGS_processed = "C:/Users/mhartman/Documents/100mDataset/yfcc100m_autotags_processed.csv"
#on external HD
path_AUTOTAGS = "D:/UZH_Job/yfcc100m_dataset/yfcc100m_autotags"
path_EXIF = "D:/UZH_Job/yfcc100m_dataset/yfcc100m_exif"
# path_AUTOTAGS = "C:/Users/mhartman/Documents/100mDataset/yfcc100m_autotags"
path_PLACES = "C:/Users/mhartman/Documents/100mDataset/yfcc100m_places"
path_db_psw = "C:/Users/mhartman/Documents/100mDataset/db_password.txt"
path_export_csv = "C:/Users/mhartman/Documents/100mDataset/tags_export_switzerland_w_user_nsid.csv"
path_logfile_db_population = "C:/Users/mhartman/Documents/100mDataset/db_population_log.txt"

maxInt = sys.maxsize

while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

def create_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_100m (
                        line_nr BIGINT PRIMARY KEY,
                        id BIGINT PRIMARY KEY,
                        id_hash TEXT,
                        user_nsid TEXT,
                        user_nickname TEXT,
                        date_taken TEXT,
                        date_uploaded BIGINT,
                        capture_device TEXT,
                        title TEXT,
                        description TEXT,
                        user_tags TEXT,
                        machine_tags TEXT,
                        lng DOUBLE PRECISION,
                        lat DOUBLE PRECISION,
                        accuracy INT,
                        page_url TEXT,
                        download_url TEXT,
                        license_name TEXT,
                        license_url TEXT,
                        media_server_identifier INT,
                        media_farm_identifier INT,
                        media_secret TEXT,
                        media_secret_original TEXT,
                        extension TEXT,                     
                        media_marker INT,
                        georeferenced INT                      
                        );""")

        #create indexes
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS index_geometry
        ON data_100m
        USING GIST(geometry);
        """)
        cursor.execute("""
                CREATE INDEX IF NOT EXISTS index_autotags
                ON public.data_100m USING btree
                (autotags COLLATE pg_catalog."C" varchar_ops ASC NULLS LAST)
                TABLESPACE pg_default;
                """)
    conn.commit()

def connect_db():
    with open(path_db_psw, 'r') as f:
        password = f.read()
    conn = psycopg2.connect("host=localhost dbname=100m_dataset user=postgres password={}".format(password))
    return conn

def update_db_flickrapi(conn, bbox, min_upload_date):
    '''
    Import
    FlickrAPI manager from different project
    '''
    from query_flickr_api import FlickrQuerier
    print('Imported FlickrQuerier')
    flickr = FlickrQuerier(bbox=bbox, min_upload_date=min_upload_date)
    data_bucket = flickr.data_bucket
    df = pd.DataFrame(data_bucket)
    df.to_pickle('./db_update_europe.pkl')
    print(f"Dataframe saved.")

    for index, row in df.iterrows():
        print(f"Test: {row['new_data']}")
        with conn.cursor() as cursor:
            cursor.execute("""INSERT INTO data_100m
            (line_nr,
            photo_id,
            id_hash,
            user_nsid,
            user_nickname,
            date_taken,
            date_uploaded,
            capture_device,
            title,
            description,
            user_tags,
            machine_tags,
            lng,
            lat,
            accuracy,
            page_url,
            download_url,
            license_name,
            license_url,
            media_server_identifier,
            media_farm_identifier,
            media_secret,
            media_secret_original,
            extension,
            media_marker,
            georeferenced,
            autotags,
            new_data
            )            
            VALUES 
            (%(line_nr)s,
            %(photo_id)s,
            %(id_hash)s,
            %(user_nsid)s,
            %(user_nickname)s,
            %(date_taken)s,
            %(date_uploaded)s,
            %(capture_device)s,
            %(title)s,
            %(description)s,
            %(user_tags)s,
            %(machine_tags)s,
            %(lng)s,
            %(lat)s,
            %(accuracy)s,
            %(page_url)s,
            %(download_url)s,
            %(license_name)s,
            %(license_url)s,
            %(media_server_identifier)s,
            %(media_farm_identifier)s,
            %(media_secret)s,
            %(media_secret_original)s,
            %(extension)s,
            %(media_marker)s,
            %(georeferenced)s,
            %(autotags)s,
            %(new_data)s
            )
            ON CONFLICT DO NOTHING;""",
                           {
                               'line_nr': row['line_nr'],
                               'photo_id': row['photo_id'],
                               'id_hash': row['id_hash'],
                               'user_nsid': row['user_nsid'],
                               'user_nickname': row['user_nickname'],
                               'date_taken': row['date_taken'],
                               'date_uploaded': row['date_uploaded'],
                               'capture_device': row['capture_device'],
                               'title': row['title'],
                               'description': row['description'],
                               'user_tags': row['user_tags'],
                               'machine_tags': row['machine_tags'],
                               'lng': row['lng'],
                               'lat': row['lat'],
                               'accuracy': row['accuracy'],
                               'page_url': row['page_url'],
                               'download_url': row['download_url'],
                               'license_name': row['license_name'],
                               'license_url': row['license_url'],
                               'media_server_identifier': row['media_server_identifier'],
                               'media_farm_identifier': row['media_farm_identifier'],
                               'media_secret': row['media_secret'],
                               'media_secret_original': row['media_secret_original'],
                               'extension': row['extension'],
                               'media_marker': row['media_marker'],
                               'georeferenced': row['georeferenced'],
                               'autotags': row['autotags'],
                               'new_data': row['new_data']
                           })
        if index % 100000 == 0:
            print("Commit")
            conn.commit()

def random_select_query(conn):
    with conn.cursor() as cursor:
        cursor.execute("""SELECT id FROM data_100m
                            WHERE autotags IS NULL
                            AND georeferenced = 1;""")
        # rows = cursor.rowcount
        for index, element in enumerate(cursor):
            print("Nr. {}: {}".format(index, element[0]))
        # response = cursor.fetchall()
    # print("Response: {}".format(response))

def display_textfile_data():
    with open(path_EXIF, 'r') as database:
        for index, line in enumerate(database):
            print(line)

            if index >= 10:
                break

def populate_db(conn, table_name, data_file):
    with open(data_file, 'r', encoding='UTF-8') as f:
        data = csv.reader(f, delimiter='\t')
        for index, line in enumerate(data):
            try:
                if index % 10000 == 0:
                    print(index)
                line_nr = line[0]
                id = line[1]
                id_hash = line[2]
                user_nsid = line[3]
                user_nickname = line[4]
                date_taken = line[5]
                date_uploaded = line[6]
                capture_device = line[7]
                title = line[8]
                description = line[9]
                user_tags = line[10]
                machine_tags = line[11]
                lng = line[12]
                if lng == '':
                    lng = 999999
                lat = line[13]
                if lat == '':
                    lat = 999999
                accuracy = line[14]
                if accuracy == '':
                    accuracy = 999999
                page_url = line[15]
                # print('{}: {}'.format(index, page_url))
                # # if page_url == '':
                # #     page_url == 'N'
                download_url = line[16]
                license_name = line[17]
                license_url = line[18]
                media_server_identifier = line[19]
                media_farm_identifier = line[20]
                media_secret = line[21]
                media_secret_original = line[22]
                extension = line[23]
                media_marker = line[24]

                if accuracy == 999999:
                    georeferenced = 0  #0 no
                else:
                    georeferenced = 1  #1 yes

                with conn.cursor() as cursor:

                    cursor.execute("""INSERT INTO data
                    (line_nr,
                    id,
                    id_hash,
                    user_nsid,
                    user_nickname,
                    date_taken,
                    date_uploaded,
                    capture_device,
                    title,
                    description,
                    user_tags,
                    machine_tags,
                    lng,
                    lat,
                    accuracy,
                    page_url,
                    download_url,
                    license_name,
                    license_url,
                    media_server_identifier,
                    media_farm_identifier,
                    media_secret,
                    media_secret_original,
                    extension,
                    media_marker,
                    georeferenced
                    )            
                    VALUES 
                    (%(line_nr)s,
                    %(id)s,
                    %(id_hash)s,
                    %(user_nsid)s,
                    %(user_nickname)s,
                    %(date_taken)s,
                    %(date_uploaded)s,
                    %(capture_device)s,
                    %(title)s,
                    %(description)s,
                    %(user_tags)s,
                    %(machine_tags)s,
                    %(lng)s,
                    %(lat)s,
                    %(accuracy)s,
                    %(page_url)s,
                    %(download_url)s,
                    %(license_name)s,
                    %(license_url)s,
                    %(media_server_identifier)s,
                    %(media_farm_identifier)s,
                    %(media_secret)s,
                    %(media_secret_original)s,
                    %(extension)s,
                    %(media_marker)s,
                    %(georeferenced)s
                    )
                    ON CONFLICT DO NOTHING;""",
                       {
                            'table_name': table_name,
                            'line_nr': line_nr,
                            'id': id,
                            'id_hash': id_hash,
                            'user_nsid': user_nsid,
                            'user_nickname': user_nickname,
                            'date_taken': date_taken,
                            'date_uploaded': date_uploaded,
                            'capture_device': capture_device,
                            'title': title,
                            'description': description,
                            'user_tags': user_tags,
                            'machine_tags': machine_tags,
                            'lng': lng,
                            'lat': lat,
                            'accuracy': accuracy,
                            'page_url': page_url,
                            'download_url': download_url,
                            'license_name': license_name,
                            'license_url': license_url,
                            'media_server_identifier': media_server_identifier,
                            'media_farm_identifier': media_farm_identifier,
                            'media_secret': media_secret,
                            'media_secret_original': media_secret_original,
                            'extension': extension,
                            'media_marker': media_marker,
                            'georeferenced': georeferenced
                        })
                if index % 100000 == 0:
                    print("Commit")
                    conn.commit()

            except Exception as e:
                print("Error: {}".format(e))
                try:
                    print("write to error log.")
                    with open(path_logfile_db_population, 'a', encoding="UTF-8") as error_log:
                        error_log.write(str(e))
                        error_log.write('\n')
                except:
                    continue

                continue

def populate_db_copy(conn, table_name, data_file):
    with conn.cursor() as cursor:
        with open(data_file, 'r') as f:
            print("Start copying...")
            cursor.copy_from(f, table_name, sep='\t')
            print("Finished copying")
    conn.commit()

def new_add_autotags(conn):
    #get all lines that need upading
    with conn.cursor() as cursor:
        cursor.execute("""SELECT id FROM data_100m 
                            WHERE autotags IS NULL
                            AND georeferenced = 1;""")
        results = cursor.fetchall()

    file_storage = {}
    print("---" * 20)
    print("Reading AUTOTAGS file into dictionary...")
    with open(path_AUTOTAGS, 'r') as dataset:
        reader = csv.reader(dataset, delimiter="\t")
        for index, line in enumerate(reader):
            storage = {}

            uid = int(line[0])

            # if uid in georeferenced_ids:

            tags = line[1]

            pointer_before = 0
            pointer_after = 0

            for char in tags:
                # before : is  the key
                if char == ':':
                    key = tags[pointer_before:pointer_after]
                    pointer_after += 1
                    pointer_before = pointer_after

                # before , is the value
                elif char == ',':
                    value = tags[pointer_before:pointer_after]
                    pointer_after += 1
                    pointer_before = pointer_after
                    storage[key] = float(value)

                else:
                    pointer_after += 1
                    continue

            # adding the last key value pair
            storage[key] = float(value)

            # store serialised as a json dump -> can be loaded again with 'json.loads'
            json_dump = json.dumps(storage)

            # store the entire line under its id in the bigger file storage
            file_storage[uid] = json_dump
    for result in results:
        pass

def add_autotags(conn):

    print("Created column 'autotags' if it did not exist already")

    file_storage = {}
    print("---" * 20)

    print("Reading AUTOTAGS file into dictionary...")
    with open(path_AUTOTAGS, 'r') as dataset:
        with open(path_AUTOTAGS_processed, 'w') as processed_f:
            reader = csv.reader(dataset, delimiter="\t")
            for index, line in enumerate(reader):
                storage = {}

                uid = int(line[0])

                # if uid in georeferenced_ids:

                tags = line[1]

                pointer_before = 0
                pointer_after = 0

                for char in tags:
                    #before : is  the key
                    if char == ':':
                        key = tags[pointer_before:pointer_after]
                        pointer_after += 1
                        pointer_before = pointer_after

                    #before , is the value
                    elif char == ',':
                        value = tags[pointer_before:pointer_after]
                        pointer_after += 1
                        pointer_before = pointer_after
                        storage[key] = float(value)

                    else:
                        pointer_after += 1
                        continue

                #adding the last key value pair
                storage[key] = float(value)

                #store serialised as a json dump -> can be loaded again with 'json.loads'
                json_dump = json.dumps(storage)

                #store the entire line under its id in the bigger file storage
                file_storage[uid] = json_dump

                #delete processed id from georeferenced ids list
                # georeferenced_ids.remove(uid)

                if index % 500000 == 0:
                    print("Processed line {index}".format(index=index))
                    #tell garbage collector to clear unreferenced memory
                    gc.collect()
                    # print("Len georeferenced id list: {}".format(len(georeferenced_ids)))

                processed_f.write("{};{}\n".format(uid, json_dump))

    gc.collect()

    print("---" * 20)
    print(">File read!")

    #SQL query to database to get all rows where AUTOTAGS is still empty
    print("---"*20)
    print("Requesting rows from db which need updating")

    with conn.cursor() as cursor_test:
        cursor_test.execute("""SELECT x.id FROM data_100m as x
                            WHERE autotags IS NULL
                            AND georeferenced = 1;""")

        #could also just iterate over the cursor itself and fetch one record after another instead of loop / fetchone()
        for index_test, element in enumerate(cursor_test): #range(cursor_rows)
            try:
                uid = int(element[0])
                #update db table according to the saved AUTOTAGS under the returned ids in the file storage dictionary

                cursor_test.execute("""UPDATE data_100m SET autotags = %(json_dump)s
                                    WHERE id = %(uid)s;""", {
                    'json_dump': file_storage[uid],
                    'uid': uid
                })

                # print("Commited Requests: {}".format(index))
                if index_test % 1000 == 0 and index_test != 0:
                    conn.commit()
                    gc.collect()
                    print("Commited Requests: {}".format(index_test))

            except Exception as e:
                print("---"*20)
                print("Error occurred: {}".format(e))
                print("---"*20)


    conn.commit()

def export_query_to_csv(conn):

    query = """SELECT x.id, x.user_nsid , x.user_tags, x.machine_tags, x.autotags, x.lng, x.lat
    FROM data_100m as x
    JOIN switzerland as y
    ON ST_WITHIN(x.geometry, y.geom)
    WHERE x.georeferenced = 1"""

    with conn.cursor() as cursor:
        outputquery = "COPY ({0}) TO STDOUT WITH DELIMITER ';' CSV HEADER".format(query)

        with open(path_export_csv, 'w') as f:
            cursor.copy_expert(outputquery, f)

    print("Export file created at: {}".format(path_export_csv))

def read_processed_autotags(conn):
    file_storage = {}
    with open(path_AUTOTAGS_processed, 'r') as processed_f:
        reader = csv.reader(processed_f, delimiter=';')
        for index, row in enumerate(reader):
            id = row[0]
            tag_dict = json.loads(row[1])
            file_storage[id] = tag_dict

            if index % 500000 == 0:
                print("Read Progress: {}".format(index))

    # with conn.cursor() as cursor_test:
    cursor = conn.cursor()
    cursor.execute("""SELECT x.id FROM data_100m as x WHERE autotags IS NULL AND georeferenced = 1;""")

    for index, element in enumerate(cursor):
        try:
            uid = int(element[0])
            cursor.execute("""UPDATE data_100m SET autotags = %(json_dump)s
                                WHERE id = %(uid)s;""", {
                'json_dump': file_storage[uid],
                'uid': uid
            })

            if index % 1000 == 0 and index != 0:
                conn.commit()
                print("Commited Requests: {}".format(index))

        except Exception as e:
            print("---"*20)
            print("Error occurred: {}".format(e))
            print("---"*20)

    conn.commit()
    cursor.close()

def read_processed_autotags_new(conn):
    try:
        with conn.cursor() as cursor:
            with open(path_AUTOTAGS_processed, 'r') as processed_f:
                reader = csv.reader(processed_f, delimiter=';')
                for index, row in enumerate(reader):
                    id = row[0]
                    tag_dict = row[1] #json.loads() for later when a dict object is required


                    #populate seperate table

                    # cursor.execute("""INSERT INTO autotags_100m (id, autotags)
                    #                 VALUES (%(id)s, %(tags)s) ON CONFLICT DO NOTHING;""", {'id': id, 'tags': tag_dict})

                    #update exisiting table
                    cursor.execute("""UPDATE data_100m SET autotags = %(tags)s
                                    WHERE id = %(id)s;""", {'id': id, 'tags': tag_dict})

                    if index % 100 == 0 and index != 0:
                        conn.commit()
                        print("Commited Requests: {}".format(index))
                        # sys.exit(0)
        conn.commit()

    except Exception as e:
        print("---"*20)
        print("Error occurred: {}".format(e))
        print("---"*20)

def set_frequency_autotags():
    path_output_unique_tags = "C:/Users/mhartman/Documents/100mDataset/unique_tags_frequency.csv"
    tags_list = []
    with open(path_AUTOTAGS_processed, 'r') as processed_f:
        reader = csv.reader(processed_f, delimiter=';')
        for index, line in enumerate(reader):
            tag_dict = json.loads(line[1])
            [tags_list.append(key) for key in tag_dict.keys()]
            if index % 10000 == 0:
                print("Line {}".format(index))

        # [tags_list.append(key) for index, line in enumerate(reader)
        #                             for key in json.loads(line[1]).keys()]
    print("Creating Counter object...")
    counter = Counter(tags_list)
    print("Writing output file...")
    with open(path_output_unique_tags, 'w') as output:
        for unique_tag in counter.most_common():
            output.write("{},{}\n".format(unique_tag[0], unique_tag[1]))

def get_images(conn):
    path_imagestoget = "C:/Users/mhartman/Documents/100mDataset/forest_snow_robot_uids.txt"
    path_saveimages = "C:/Users/mhartman/Documents/100mDataset/forest_snow_robot_images/"
    ids_toget = []
    with open(path_imagestoget, 'r') as f:
        reader = csv.reader(f, delimiter=",")
        for index1, line in enumerate(reader):
            if index1 == 0:
                continue
            id_toget = line[1]
            ids_toget.append(id_toget)

    print("File read - ids retrieved")

    with conn.cursor() as cursor:
        print("Querying db for download urls...")
        cursor.execute("""SELECT x.download_url FROM data_100m as x WHERE x.id IN %(toget)s""", {'toget': tuple(ids_toget)})

        for index, url in enumerate(cursor):
            try:
                #SSL certificate error - turn http: into https:
                url = url[0]
                url_https = 'https' + url[4:]
                urllib.request.urlretrieve(url_https, path_saveimages, context=ssl._create_unverified_context())
                # resource = urllib.request.urlopen(url_https, context=ssl._create_unverified_context())
                #
                # with open(path_saveimages + f"image{index}.jpg", 'wb') as image:
                #     image.write(resource.read())

                print(f"retrieved {index} images from {cursor.rowcount}")
            except Exception as e:
                print(f"image not found: {e}")

def read_exif():
    storage = {}
    with open(path_EXIF, 'r') as database:
        for index, line in enumerate(database):
            # print(line)
            split = line.split(':')
            index = 0
            print(f"len: {len(split)-1}")
            for i in range(int(((len(split)-1) / 2))):
                storage[split[index].replace('+', ' ')] = split[index + 1].replace('+', ' ')
                index += 2

            for key in storage:
                print(f"{key:<60} : {storage[key]}")

            break

            if index >= 10:
                break

conn = connect_db()

# create_table(connect_db())
# display_textfile_data()
# populate_db(conn, 'data', path_DATASET)
# add_autotags(conn)
# random_select_query(conn)
# read_processed_autotags(conn)
# export_query_to_csv(conn)
# read_processed_autotags_new(conn)
# set_frequency_autotags()
# get_images(conn)
# read_exif()
bbox_europe = ['-27.0703125,34.59704151614417,42.890625,71.85622888185527']
bbox_small = ['9.414564,47.284421,9.415497,47.285627']
min_upload_date = '1398683777'
update_db_flickrapi(conn, bbox_europe, min_upload_date)

conn.commit()
conn.close()