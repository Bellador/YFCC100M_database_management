import flickrapi
import re
import json
import urllib
import ssl
import datetime
import time
import os
import hashlib
import requests
from bs4 import BeautifulSoup
from functools import wraps

class FlickrQuerier:
    '''
    IMPORTANT NOTICE:
    the flickr API does not return more than 4'000 results per query even if all returned pages are parsed.
    This is a bug/feature (https://www.flickr.com/groups/51035612836@N01/discuss/72157654309722194/)
    Therefore, queries have to be constructed in a way that less than 4'000 are returned.
    '''
    path_CREDENTIALS = "C:/Users/mhartman/PycharmProjects/MotiveDetection/FLICKR_API_KEY.txt"
    path_saveimages_wildkirchli = "C:/Users/mhartman/Documents/100mDataset/wildkirchli_images/"
    path_LOG = "C:/Users/mhartman/PycharmProjects/MotiveDetection/LOG_FLICKR_API.txt"
    # path_CSV = "C:/Users/mhartman/PycharmProjects/MotiveDetection/wildkirchli_metadata.csv"

    class Decorators:
        # decorator to wrap around functions to log if they are being called
        @classmethod
        def logit(self, func):
            #preserve the passed functions (func) identity - so I doesn't point to the 'wrapper_func'
            @wraps(func)
            def wrapper_func(*args, **kwargs):
                with open(FlickrQuerier.path_LOG, 'at') as log_f:
                    #print("Logging...")
                    log_f.write('-'*20)
                    log_f.write(f'{datetime.datetime.now()} : function {func.__name__} called \n')
                return func(*args, **kwargs)
            return wrapper_func

    def __init__(self, bbox, min_upload_date=None, max_upload_date=None):
        print("--"*30)
        print("Initialising Flickr Search with FlickrQuerier Class")
        self.bbox = bbox
        self.min_upload_date = min_upload_date
        self.max_upload_date = max_upload_date
        self.api_key, self.api_secret = self.load_creds(FlickrQuerier.path_CREDENTIALS)
        print("--" * 30)
        print(f"Loading flickr API credentials - done.")
        print("--" * 30)
        print(f"Quering flickr API with given bbox: \n{self.bbox}")
        self.unique_ids, self.flickr = self.flickr_search()
        print("--" * 30)
        print(f"Search - done.")
        '''
        Update:
        Since the Flickr API call limit is at 3600 per hour meaning ~1/s it is not feasable to
        call the API for every (of the million) found ids. Instead the 'extra' parameter for the above flickr.search
        is passed to retrieve these extra infos:
        A comma-delimited list of extra information to fetch for each returned record. Currently supported fields are: 
        description, license, date_upload, date_taken, owner_name, icon_server, original_format, last_update, geo, 
        tags, machine_tags, o_dims, views, media, path_alias, url_sq, url_t, url_s, url_q, url_m, url_n, url_z, url_c, url_l, url_o
        '''
        # print("--" * 30)
        # print(f"Fetching metadata for search results and writing to file...")
        # self.data_bucket = self.get_info()
        # print("--" * 30)
        # print(f"Acquiring metadata - done.")
        print("--" * 30)
        # print(f"Downloading images into folder {project_name} to current directory.")
        # self.get_images(self.unique_ids, self.flickr)
        # print("\n--" * 30)
        # print(f"Download images - done.")
        # print("--" * 30)
        # print("--" * 30)
        # print("FlickrQuerier Class - done")

    @Decorators.logit
    def load_creds(self, path):
        key_found = False
        secret_found = False
        with open(FlickrQuerier.path_CREDENTIALS, 'r') as f:
            for line in f:
                if key_found:
                    api_key = line.strip().encode('utf-8')
                    key_found = False

                if secret_found:
                    api_secret = line.strip().encode('utf-8')
                    secret_found = False

                if re.match(r'<KEY>', line):
                    key_found = True
                    continue
                elif re.match(r'<SECRET>', line):
                    secret_found = True
        return api_key, api_secret

    @staticmethod
    def fill_data_bucket(data_bucket, api_results, data_source, csv_separator=';', tag_connector='+'):
        '''

        :param data_bucket:
        :param api_results:
        :param csv_separator:
        :param tag_connector:
        :param data_source: name api endpoint, either 'photoSearch' or 'getInfo'
        :return:
        '''
        def remove_non_ascii(s):
            return "".join(i for i in s if ord(i) < 126 and ord(i) > 31)

        def get_download_url(page_url):
            try:
                page = requests.get(page_url)
                soup = BeautifulSoup(page.content, 'html.parser')
                search = soup.find_all('meta', property='og:image')
                download_url = search[0]['content']
                return download_url
            except Exception as e:
                print(f"Error {e} occurred")
                return ''


        '''
        Available licenses:
        According to Flickr API doc: https://www.flickr.com/services/api/flickr.photos.licenses.getInfo.html
        '''
        licenses = {1: {'name': "Attribution-NonCommercial-ShareAlike License", 'url': "https://creativecommons.org/licenses/by-nc-sa/2.0/",
                        2: }

        results = api_results['photo']
        '''
        define which info fields should be fetched.
        ERASE ALL STRINGS OF CSV SEPERATOR! 
        '''
        # extract tags into an string separated by '+'!
        try:
            if data_source == 'getInfo':
                for tag_index, tag in enumerate(results['tags']['tag']):
                    tag_string = tag_string + results['tags']['tag'][tag_index]['_content'].replace(csv_separator, '').replace(
                        tag_connector, '') + tag_connector
            elif data_source == 'photoSearch':
                tag_string = results['tags'].replace(tag_connector, '')
        except Exception as e:
            tag_string = ''
        try:
            if data_source == 'getInfo':
                locality = results['location']['locality']['_content'].replace(csv_separator, '')
            elif data_source == 'photoSearch':
                locality = ''
        except Exception as e:
            locality = ''
            print(f"{e} not found. Continue")
        try:
            if data_source == 'getInfo':
                county = results['location']['county']['_content'].replace(csv_separator, '')
            elif data_source == 'photoSearch':
                county = ''
        except Exception as e:
            county = ''
            # print(f"{e} not found. Continue")
        try:
            if data_source == 'getInfo':
                region = results['location']['region']['_content'].replace(csv_separator, '')
            elif data_source == 'photoSearch':
                region = ''
        except Exception as e:
            region = ''
            # print(f"{e} not found. Continue")
        try:
            if data_source == 'getInfo':
                country = results['location']['country']['_content'].replace(csv_separator, '')
            elif data_source == 'photoSearch':
                county = ''
        except Exception as e:
            country = ''
        '''
        text clean up
        of title and description
        - remove linebreaks etc.
        '''
        if data_source == 'getInfo':
            description = remove_non_ascii(results['description']['_content'].replace(csv_separator, ''))
            title = remove_non_ascii(results['title']['_content'].replace(csv_separator, ''))
            '''
            Creating MD5
            hash of photo_id
            '''
            photo_id = results['id']
        elif data_source == 'photoSearch':
            description = results['description']
            title = results['title']
            photo_id = results['id']
        try:
            hash_result = hashlib.md5(str.encode(photo_id))
        except:
            hash_result = ''
        '''
        Get URLs
        page & download url
        '''
        try:
            if data_source == 'getInfo':
                page_url = results['urls']['url'][0]['_content'].replace(csv_separator, '')
            elif data_source == 'photoSearch':
                page_url = ''
        except:
            page_url = ''
        try:
            if data_source == 'getInfo':
                download_url = get_download_url(page_url)
            elif data_source == 'photoSearch':
                download_url = results['url_c']
        except:
            download_url = ''
        try:
            media_extension = download_url[-3:]
        except:
            media_extension = ''
        if data_source == 'getInfo':
            data = {
                'line_nr': 999999,
                'photo_id': photo_id,
                'id_hash': hash_result.hexdigest(),
                'user_nsid': results['owner']['nsid'].replace(csv_separator, ''),
                'user_nickname': results['owner']['username'].replace(csv_separator, ''),
                'date_taken': results['dates']['taken'].replace(csv_separator, ''),
                'date_uploaded': results['dates']['posted'].replace(csv_separator, ''),
                'capture_device': '',
                'title': title,
                'description': description,
                'user_tags': tag_string,
                'machine_tags': '',
                'lng': results['location']['longitude'].replace(csv_separator, ''),
                'lat': results['location']['latitude'].replace(csv_separator, ''),
                'accuracy': results['location']['accuracy'].replace(csv_separator, ''),
                'page_url': page_url,
                'download_url': download_url,
                'license_name': 'Attribution License',
                'license_url': 'http://creativecommons.org/licenses/by/2.0/',
                'media_server_identifier': 0,
                'media_farm_identifier': 0,
                'media_secret': 'None',
                'media_secret_original': 'None',
                'extension': media_extension,
                'media_marker': 0,
                'georeferenced': 1,
                'autotags': 'None',
                'new_data': 1
                }
        elif data_source == 'photoSearch':
            data = {
                'line_nr': 999999,
                'photo_id': photo_id,
                'id_hash': hash_result.hexdigest(),
                'user_nsid': results['owner']['nsid'].replace(csv_separator, ''),
                'user_nickname': results['owner']['username'].replace(csv_separator, ''),
                'date_taken': results['dates']['taken'].replace(csv_separator, ''),
                'date_uploaded': results['dates']['posted'].replace(csv_separator, ''),
                'capture_device': '',
                'title': title,
                'description': description,
                'user_tags': tag_string,
                'machine_tags': '',
                'lng': results['location']['longitude'].replace(csv_separator, ''),
                'lat': results['location']['latitude'].replace(csv_separator, ''),
                'accuracy': results['accuracy'].replace(csv_separator, ''),
                'page_url': page_url,
                'download_url': download_url,
                'license_name': results['location'],
                'license_url': ,
                'media_server_identifier': 0,
                'media_farm_identifier': 0,
                'media_secret': 'None',
                'media_secret_original': 'None',
                'extension': media_extension,
                'media_marker': 0,
                'georeferenced': 1,
                'autotags': 'None',
                'new_data': 1
            }
        data_bucket.append(data)
        return data_bucket

    def flickr_search(self):
        '''
        Idea to query all items for europe:
        Iterate over the timestamps (min - max) to construct sub-queries.
        30.07.2019 unix-timestamp: 1564444800
        :return:

        In yearly steps up to 2019:
        1. 1398683777 (4.2014)  -   1414454400 (10.2014)
        2. 1414454400 (10.2014) -   1430179200 (2015)
        '''
        allowed_licenses = '1,2,3,4,5,6,7'
        extra_data = 'description,license,date_upload,date_taken,owner_name,geo,tags,machine_tags,url_c'
        error_sleep_time = 60
        result_dict = {}
        total_pages = 0
        t_diff = 100000 #seconds
        #track max amount of results per query - keeping check of not making to be queries
        max_total = 0
        flickr = flickrapi.FlickrAPI(self.api_key, self.api_secret, format='json')
        self.min_upload_date = int('1398683777')
        self.max_upload_date = int('1414454400')
        steps = self.max_upload_date - self.min_upload_date
        print(f"{round(steps/t_diff)} timesteps to be queried...")
        for query_counter, (step_lower, step_upper) in enumerate(zip(range(self.min_upload_date, self.max_upload_date, t_diff), range((self.min_upload_date+t_diff), (self.max_upload_date+t_diff), t_diff))):
            print(f"\rTimespan {step_lower} to {step_upper}, {query_counter} of {round((steps/t_diff))}  ", end='')
            try:
                photos = flickr.photos.search(bbox=self.bbox, min_upload_date=str(step_lower), max_upload_date=str(step_upper), per_page=250, extras=extra_data, license=allowed_licenses) #is_common="True" #is_, accuracy=12, commons=True, page=1, min_taken_date='YYYY-MM-DD HH:MM:SS'
                result = json.loads(photos.decode('utf-8'))
                print(json.dumps(result, indent=2))
                '''
                Handling for multipage results stored in result_dict
                '''
                total_pages += 1
                pages = result['photos']['pages']
                total = int(result['photos']['total'])
                if total > max_total:
                    max_total = total
                result_dict[f'page_{total_pages}'] = result
            except Exception as e:
                print("*" * 30)
                print("*" * 30)
                print("Error occurred: {}".format(e))
                print(f"sleeping {error_sleep_time}s...")
                print("*" * 30)
                print("*" * 30)
                time.sleep(60)
                continue
            if pages != 1 and pages != 0:
                print(f" - Search returned {pages} result pages")
                for page in range(2, pages+1):
                    total_pages += 1
                    print(f"\rQuerying page {page}...", end='')
                    try:
                        result_bytes = flickr.photos.search(bbox=self.bbox, min_upload_date=str(step_lower),
                                             max_upload_date=str(step_upper), extras=extra_data, license=allowed_licenses, page=page, per_page=250)
                        page_result = json.loads(result_bytes.decode('utf-8'))
                        result_dict[f'page_{total_pages}'] = page_result
                        total = int(page_result['photos']['total'])
                        if total > max_total:
                            max_total = total
                    except Exception as e:
                        print("*" * 30)
                        print("*" * 30)
                        print("Error occurred: {}".format(e))
                        print(f"sleeping {error_sleep_time}s...")
                        print("*" * 30)
                        print("*" * 30)
                        time.sleep(60)

        print("All timesteps queried")
        #get ids of returned flickr images
        ids = []
        for dict_ in result_dict:
            for element in result_dict[dict_]['photos']['photo']:
                ids.append(element['id'])
        unique_ids = set(ids)
        print(f"Total results found: {len(unique_ids)}")
        return unique_ids, flickr

    def get_images(self, ids, flickr):
        self.image_path = os.path.join(self.project_path, f'images_{self.project_name}')
        if not os.path.exists(self.image_path):
            os.makedirs(self.image_path)
            print(f"Creating image folder 'images_{self.project_name}' in sub-directory '/{self.project_name}/' - done.")
        else:
            print(f"Image folder 'images_{self.project_name}' exists already in the sub-directory '/{self.project_name}/'.")

        for index, id in enumerate(ids):
            results = json.loads(flickr.photos.getSizes(photo_id=id).decode('utf-8'))
            # print(json.dumps(json.loads(results.decode('utf-8')), indent=2))
            try:
                # Medium 640 image size url
                url_medium = results['sizes']['size'][6]['source']
                # urllib.request.urlretrieve(url_medium, path) # context=ssl._create_unverified_context()
                resource = urllib.request.urlopen(url_medium, context=ssl._create_unverified_context())
                with open(self.image_path + '/' + f"{id}.jpg", 'wb') as image:
                    image.write(resource.read())
                print(f"\rretrieved {index} of {len(ids)} images", end='')

            except Exception as e:
                print(f"image not found: {e}")

    def get_info(self):
        csv_separator = ';'
        tag_connector = '+'
        data_bucket = []
        for index, id in enumerate(self.unique_ids):
            print(f"\rProcessed {index+1} of {len(self.unique_ids)}")
            try:
                results = json.loads(self.flickr.photos.getInfo(photo_id=id).decode('utf-8'))
                # results = results['photo']
                data_bucket = FlickrQuerier.fill_data_bucket(data_bucket, results, 'getInfo', csv_separator=csv_separator, tag_connector=tag_connector)
            except Exception as e:
                print(f"{e} - No metadata found")
                print("Sleep 60s...")
                time.sleep(60)