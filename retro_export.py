import logging
import pandas as pd
from retro_helper import *

'''
File: retro_export.py
Description: This File is responsible for EXPORTING data from the Ocrolus System to a local File System
'''

###
# write_analytics
###
def write_analytics(num_threads,book_list,auth,prj_dir):
    out_dir = prj_dir + '/outbound/OcrolusAnalytics'
    threads = list()
    threadNum = 0

    for y in split(book_list['response'], num_threads):
        x = threading.Thread(target=write_analytics_thread, args=(threadNum, y,auth,out_dir))
        threads.append(x)
        x.start()
        threadNum += 1
    for index, thread in enumerate(threads):
        thread.join()

def write_analytics_thread(threadname, bl_list,auth,cust_dir):
    sleepTime = 1
    for book in bl_list:
        book_name = book['name']
        book_uuid = book['book_uuid']

        logging.info(f'{threadname=} Working On {book_uuid=} {book_name=}')
        count = 0
        while (count<3):
            count+=1
            try:
                respData = requests.get(url_v2+"/book/"+book_uuid+"/cash_flow_features",auth=auth)
                CFF_data = json.loads(json.dumps(respData.json()))
                write_file(cust_dir + '/' + book_name, 'cash_flow_features.json', CFF_data)

                #### BOOK_SUMMARY
                respData = requests.get(url_v2 + "/book/" + book_uuid + "/summary", auth=auth)
                summaryV2 = json.loads(json.dumps(respData.json()))
                write_file(cust_dir + '/' + book_name, 'analyticsV2.json', summaryV2)


                respData = requests.get(url_v2 + "/book/" + book_uuid + "/enriched_txns", auth=auth)
                enrichedTxns = json.loads(json.dumps(respData.json()))
                write_file(cust_dir + '/' + book_name, 'enrichedTxns.json', enrichedTxns)
                book_count.inc()
                time.sleep(sleepTime)
                break
            except Exception as e:
                logging.error(f'{threadname=} {book_uuid=} {book_name=} {e=}')
                logging.error(f'{book_name=} {count=} {respData.content=}')


def write_book_status(auth,prj_dir,bl_json):
    stat_dir = prj_dir+'/outbound/status/'

    for book in bl_json['response']:
        payload={'pk':str(book['pk'])}
        bsr = json.loads(
            json.dumps(requests.get("https://api.ocrolus.com/v1/book/status", auth=auth, data=payload).json()))
        write_file(stat_dir + book['name'], 'status.json', bsr)

def loadStatus(book_name,prj_dir):
    with open(prj_dir + '/outbound/status/' + str(book_name)+'/status.json', 'r') as cd:
        cd_json = json.load(cd)
        return cd_json

###  WRITE CLASSIFICATION
def write_classification_thread(threadname,bl_list,auth,class_dir):
    timeSleep=1
    for book in bl_list:
        book_uuid = book.get('book_uuid')
        book_name = book.get('name')
        logging.info(f'{book_name=} {book_uuid=}')
        count =0
        while(count<3):
            try:
                count+=1
                bcsr = json.loads(json.dumps(requests.get(url_v2 + "/book/" + book_uuid + "/classification-summary", auth=auth).json()))
                #if bcsr.get('status') != 200:
                #    logging.error(f'{book_name=} {book_uuid=} {json.dumps(bcsr)}')
                #    time.sleep(timeSleep)
                #    continue
                write_file(class_dir + book['name'] ,'class.json',bcsr)
                break
            except Exception as e:
                logging.error(f'{book_name=} {json.dumps(bcsr)}')
                break

# write_classification
def write_classification(num_threads,bl_list,auth,prj_dir):
    class_dir = prj_dir+'/outbound/classification/'
    logging.info(f'Entered {len(bl_list)=}')
    threads = list()
    threadNum = 0
    for y in split(bl_list['response'], num_threads):
        x = threading.Thread(target=write_classification_thread, args=(threadNum,y,auth,class_dir))
        threads.append(x)
        x.start()
        threadNum +=1
    for index, thread in enumerate(threads):
        thread.join()

# loadClassification JSON
def loadClassification(book_name,prj_dir):
    try:
        with open(prj_dir + '/outbound/classification/' + str(book_name)+'/class.json', 'r') as cd:
            cd_json = json.load(cd)
            return cd_json
    except Exception as e:
        logging.error(f'{book_name = } {e}')
        return None

###  WRITE DETECT
def write_book_detect_thread(threadname,bl_list,auth,class_dir):
    for book in bl_list:
        book_uuid = book.get('book_uuid')
        book_name = book.get('name')
        logging.info(f'{book_name=} {book_uuid=}')
        dr = json.loads(json.dumps(
            requests.get("https://api.ocrolus.com/v2/detect/book/"+book_uuid+"/signals", auth=auth).json()))
        write_file(class_dir + book['name'] ,'detect.json',dr)

# write_detect
def write_book_detect(num_threads,bl_list,auth,prj_dir):
    class_dir = prj_dir+'/outbound/Detect/'
    logging.info(f'Entered {len(bl_list)=}')
    threads = list()
    threadNum = 0
    for y in split(bl_list['response'], num_threads):
        x = threading.Thread(target=write_book_detect_thread, args=(threadNum,y,auth,class_dir))
        threads.append(x)
        x.start()
        threadNum +=1
    for index, thread in enumerate(threads):
        thread.join()

def write_paystub_data(auth,book_list,prj_dir):
    ps_dir = prj_dir + '/outbound/Paystub/'
    print('f')

    for book in book_list.get('response'):
        book_name = book.get('name')
        book_uuid = book.get('book_uuid')
        book_pk = book.get('book_pk')

        ## https://api.ocrolus.com/v2/book/{book_uuid}/paystub
        respData = requests.get(url_v2 + "/book/" + book_uuid + "/paystub", auth=auth)
        ps_book_json = json.loads(json.dumps(respData.json()))
        write_file(ps_dir + book_name,'bookPaystub.json',ps_book_json)

def write_risk_score(auth,bl_csv,prj_dir,book_list):
    rs_dir = prj_dir + '/outbound/RiskScore/'
    bl = book_list.get('response')
    df = pd.read_csv(bl_csv)

    for index, row in df.iterrows():
        respData = requests.get(url_v2 + "/book/" + row.book_uuid + "/cash_flow_risk_score", auth=auth)
        bl_entry = list(filter(lambda book: book['book_uuid'] == row.book_uuid, bl))[0]
        risk_json = json.loads(json.dumps(respData.json()))
        write_file(prj_dir + '/outbound/RiskScore/' + bl_entry.get('name')+'/' , 'risk_score.json', risk_json)
        print(f"{bl_entry.get('name')}")
        #https://api.ocrolus.com/v2/book/{book_uuid}/cash_flow_risk_score'

def write_form_data(auth,cust_prj):

    bl_json = get_booklist(auth,cust_prj)

    for x in bl_json['response']:
        cd_json = loadClassification(x['name'],cust_prj)
        if cd_json is None or cd_json.get('response') is None:
            logging.error(f'{x["name"]} had issues with loading classification ')
            continue

        for y in cd_json.get('response').get('forms'):
            if y.get('form_type').get('name') not in ('PAYSTUB','BANK_ACCOUNT','UNKNOWN'):
                payload = {'uuid': str(y['form_uuid'])}
                dfd = json.loads(
                    json.dumps(
                        requests.get("https://api.ocrolus.com/v1/form", auth=auth, data=payload).json()))
                write_file(cust_prj + '/outbound/formData/' + x['name'] +'/' ,
                           str(y['form_type']['name']) + '_' + y.get('form_uuid') + '.json', dfd)
            elif y.get('form_type').get('name') in ('PAYSTUB'):
                psd = json.loads(
                    json.dumps(
                        requests.get("https://api.ocrolus.com/v2/paystub/" + str(y['form_uuid']), auth=auth).json()))
                write_file(cust_prj + '/outbound/formData/' + x['name'] + '/',
                           str(y['form_type']['name']) + '_' + y.get('form_uuid') + '.json', psd)

    print('write_form_data')
