import logging
import requests,json
import os
from pathlib import Path
import threading
import time
from requests.auth import HTTPBasicAuth
import pandas as pd

'''
Module: retro_helper.py
Description: This module is responsible for 
'''

url = "https://api.ocrolus.com/v1"
url_v2 = "https://api.ocrolus.com/v2"

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

def get_bearer_token(client_id, client_secret):
    res = requests.post("https://auth.ocrolus.com/oauth/token", data={
        "grant_type": "client_credentials",
        "audience": "https://api.ocrolus.com/",
        "client_id": client_id,
        "client_secret": client_secret
    })
    auth = BearerAuth(res.json()["access_token"])
    return auth

def get_auth(filePath):
    with open(filePath, 'r') as oa_file:
        oa_json = json.loads(oa_file.read())
    return get_bearer_token(oa_json.get('clientId'), oa_json.get('clientSecret'))


def get_basic_auth(username,password):
    return HTTPBasicAuth(username,password)

def write_file(folder, fileName, rep_json):
    if not Path(folder).is_dir():
        os.mkdir(folder)
    try:
        with open(folder + '/' + fileName, 'w') as outFile:
            outFile.write(json.dumps(rep_json, indent=4))
    except:
        logging.error(f"Issue writing {fileName=}")

###
#   get_booklist(auth, cust_dir,read_file=False)
###
def get_booklist(auth,cust_dir,read_file=False):

    filePath = cust_dir+'/outbound/classification/'
    if read_file and Path(filePath + '/book_list.json').is_file():
        with open(cust_dir + '/book_list.json', 'r') as bl_file:
            bl_json = json.loads(bl_file.read())
            return bl_json
    else:
        bl_json  = json.loads(json.dumps(requests.get(url + '/books', auth=auth).json()))
        write_file(filePath, 'book_list.json', bl_json)
        return bl_json


class AtomicCounter(object):
    def __init__(self, initial=0):
        self._value = initial
        self._lock = threading.Lock()
    def inc(self, num=1):
        with self._lock:
            self._value += num
            if self._value % 10 == 0:
                print(f'{book_count.value=}')
    @property
    def value(self):
        return self._value

book_count = AtomicCounter()


def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def configure_logging(cust_dir,file_name):
    logging.basicConfig(filename=cust_dir + '/'+file_name,
                        format='%(asctime)s %(levelname)s %(module)s:%(funcName)s():%(lineno)s %(message)s',
                        encoding='utf-8', level=logging.INFO)
    logging.info('Starting Processing')


###
# getDirectoryHierarchy
###
def getDirectoryHierarchy(directory):
    hierarchy = []

    for root, dirs, files in os.walk(directory):
        subDir = {"directory": root, "files": []}
        if (root != directory):
            hierarchy.append(subDir)
            for filename in files:
                subDir['files'].append(filename)

    df = pd.DataFrame(hierarchy)
    df = df.explode('files').reset_index()
    df = df.rename({'files': 'file'}, axis=1)
    df['application_name'] = df['directory'].str.split(directory).str[1]
    return df

def flattenDict(d,result=None,index=None,Key=None):
    if result is None:
        result = {}
    if isinstance(d, (list, tuple)):
        for indexB, element in enumerate(d):
            if Key is not None:
                newkey = Key
            flattenDict(element,result,index=indexB,Key=newkey)
    elif isinstance(d, dict):
        for key in d:
            value = d[key]
            if Key is not None and index is not None:
                newkey = "_".join([Key,(str(key).replace(" ", "") + str(index))])
            elif Key is not None:
                newkey = "_".join([Key,(str(key).replace(" ", ""))])
            else:
                newkey= str(key).replace(" ", "")
            flattenDict(value,result,index=None,Key=newkey)
    else:
        result[Key]=d
    return result

def deleteBooksFromFile(auth,filepath):
    df = pd.read_csv(filepath,dtype=str)
    print('f')
    for index, row in df.iterrows():
        payload = {'book_id': row.pk}
        dbr = json.loads( json.dumps(requests.post(url + '/book/remove', auth=auth, data=payload).json()))
        logging.info(f'Response: {json.dumps(dbr, indent=4)}')

def createBook(book_name,auth):
    # CREATE BOOK
    data = {'name': book_name}

    try:
        cbr = json.loads(
            json.dumps(requests.post('https://api.ocrolus.com/v1/book/add', data=data, auth=auth).json()))

        logging.info(f'Create Book {book_name} Response: {json.dumps(cbr, indent=4)}')
        book_uuid = cbr.get('response').get('uuid')
        book_pk = cbr.get('response').get('pk')
        return book_uuid
    except Exception as e:
        logging.error(f'{e}')
        return None
