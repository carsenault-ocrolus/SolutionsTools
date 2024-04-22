import logging
import pandas as pd
from retro_helper import *

'''
File: retro_flatten.py
Description: This module is responsible for flattening data (into csv)
'''

#####
##    flatten_book_summary
#####
def flatten_book_summary(df,out_dir):
    df = df[df['file'].str.contains('analyticsV2')]

    # FLATTEN Analytics to 1 LINE PER BOOK-MONTH.  (LESS WIDE FORMAT)
    bookListScalar = list()
    monthList = list()
    for index, row in df.iterrows():
        with open(row.directory + '/' + row.file, 'r') as ds:
            ds_json = json.load(ds)
        book_pk = ds_json.get('book_pk')
        book_name = ds_json.get('book_name')
        aKeys = list(ds_json.keys())
        for x in ['bank_accounts', 'uploaded_docs', 'daily_balances', 'daily_cash_flows']:
            if x in aKeys:
                aKeys.remove(x)

        # BUILD TABLE BY MONTH (SCALARS ONLY)
        mScalDict = {}
        for x in aKeys:
            val = ds_json.get(x)

            if type(val) in (int, str, float):
                mScalDict[x] = val
        bookListScalar.append(mScalDict)

        # BUILD TABLE BY MONTH - BOOK
        if ds_json.get('average_daily_balance_by_month') is not None:
            for mKey in ds_json.get('average_daily_balance_by_month'):
                my_dict = {}
                for x in ['book_pk', 'book_name']:
                    my_dict[x] = ds_json.get(x)
                my_dict['month'] = mKey

                for x in aKeys:
                    y = ds_json.get(x)
                    if type(y) in (int, str, float):
                        pass
                        # my_dict[x] = y
                    elif type(y) is dict:
                        # if len(y)>25:
                        #    break

                        val = y.get(mKey)
                        if type(val) in (int, str, float):
                            my_dict[x] = val
                        elif type(val) is list:
                            my_dict[x] = len(val)
                    else:
                        pass
                        ### NEED TO HANDLE EMPTY ANALYTICS TODO CJA

                monthList.append(my_dict)

    blDF = pd.DataFrame.from_records(monthList)
    blDF.to_csv(out_dir + '/flatMonthly.csv')
    scalDF = pd.DataFrame.from_records(bookListScalar)
    scalDF.to_csv(out_dir + '/flatScalars.csv')


#####
##    flatten_cash_flow
#####
def flatten_cash_flow(df,out_dir):
    df = df[df['file']=='cash_flow_features.json']
    cffList= list()

    masterDF = pd.DataFrame()
    for index, row in df.iterrows():
        rowDict={}
        with open(row.directory + '/' + row.file, 'r') as et:
            et_json = json.load(et)
        try:
            for x in ['book_uuid','book_start_month','book_end_month','num_of_months']:
                rowDict[x]=et_json[x]
            rowDict['book_name'] = row.application_name

            y = et_json['cash_flow_features']
            if y is not None:
                for x in y:
                    rowDict[x]=y.get(x)
            cffList.append(rowDict)
        except Exception as e:
            print('f')

    masterDF = pd.DataFrame.from_records(cffList)
    masterDF.drop('error_messages',axis=1,inplace=True)
    masterDF.to_csv(out_dir + '/cash_flow_summary.csv',index=False)


#####
##    flatten_enriched_txns
#####
def flatten_enriched_txns(df,out_dir):
    df = df[df['file']=='enrichedTxns.json']
    txnList = list()
    masterDF = pd.DataFrame()
    for index, row in df.iterrows():
        rowDict = {}
        try:
            with open(row.directory + '/' + row.file, 'r') as et:
                et_json = json.load(et)

            for x in et_json['enriched_transactions']:
                x['book_uuid'] = et_json['book_uuid']
                txnList.append(x)
        except Exception as inst:
            logging.error(f'{row.application_name} {inst}')

    masterDF = pd.DataFrame.from_records(txnList)
    masterDF.to_csv(out_dir+'/enrichedTxns.csv',index=False)


#####
##    flatten_period
#####
def flatten_period(df,out_dir):
    df = df[df['file'].str.contains('analyticsV2')]

    periodList = list()
    for index, row in df.iterrows():
        with open(row.directory + '/' + row.file, 'r') as ds:
            ds_json = json.load(ds)

        if ds_json.get('status') is not None:
            if ds_json.get('status') in (425,400):
                logging.error(f'{row.application_name} {json.dumps(ds_json)}')
                continue

        cols = ['book_uuid','book_pk','book_name']
        book_dict = {key:ds_json.get(key) for key in cols}
        for ba in ds_json.get('bank_accounts'):
            cols=['bank_account_pk', 'bank_account_name', 'bank_name', 'account_type', 'account_holder', 'account_number', 'holder_zip', 'holder_country', 'holder_state', 'holder_city', 'holder_address_1', 'holder_address_2', 'account_category']
            bank_dict={key:ba.get(key) for key in cols}
            for period in ba.get('periods'):
                periodList.append(book_dict|bank_dict|period)
    df = pd.DataFrame.from_records(periodList)
    df.to_csv(out_dir+'/flatPeriod.csv')

#####
##    flatten_analytics
#####
def flatten_analytics(prj_dir):
    analytics_dir = prj_dir+'/outbound/OcrolusAnalytics/'
    df = getDirectoryHierarchy(analytics_dir)

    # FLATTEN CASH FLOW FEATURES
    flatten_cash_flow(df,analytics_dir)

    # FLATTEN ENRICHED TRANSACTIONS
    flatten_enriched_txns(df,analytics_dir)

    # FLATTEN BOOK SUMMARY
    flatten_book_summary(df,analytics_dir)

def flattenStatus(prj_dir):
    stat_dir = prj_dir +'/outbound/status/'
    df = getDirectoryHierarchy(stat_dir)

    doc_list = []
    md_list = []
    book_cols = ['name', 'created', 'created_ts', 'pk', 'owner_email', 'is_public', 'uuid', 'id', 'is_shared_or_public_book', 'book_status', 'book_class']
    stat_list=[]
    for index, row in df.iterrows():
        with open(row.directory + '/' + row.file, 'r') as ss:
            ss_json = json.load(ss)
        book_dict= {'book_'+key: ss_json.get('response').get(key) for key in book_cols}
        ### SPIN THROUGH DOCS
        for doc in ss_json.get('response').get('docs'):
            dDict ={'doc_'+ k: v for k, v in doc.items()}
            doc_list.append(book_dict|dDict)
        ### SPIN THROUGH MD
        for md in ss_json.get('response').get('mixed_docs'):
            mDict = {'md_' + k: v for k, v in md.items()}
            md_list.append(mDict)

    dDF = pd.DataFrame.from_records(doc_list)
    mDF = pd.DataFrame.from_records(md_list)
    mergeDF = dDF.merge(mDF, left_on='doc_mixed_uploaded_doc_pk', right_on='md_pk', how='left')
    mergeDF.to_csv(stat_dir +'/status.csv')
    mDF.to_csv(stat_dir+'/mixedDocs.csv')
    dDF.to_csv(stat_dir+'/uploadDocs.csv')
    print('f')

def flatten_classification(prj_dir):
    class_dir = prj_dir +'/outbound/classification/'
    df = getDirectoryHierarchy(class_dir)

    class_list=[]
    for index, row in df.iterrows():
        with open(row.directory + '/' + row.file, 'r') as cs:
            cs_json = json.load(cs)
        book_uuid = cs_json.get('response').get('book_uuid')

        cols= ['form_uuid', 'upload_origin', 'status', 'is_original', 'form_uuid_duplicate_of']
        ### SPIN THROUGH FORMS
        for form in cs_json.get('response').get('forms'):
            mDict = {'book_name':row.application_name,'book_uuid':cs_json.get('response').get('book_uuid')}
            mDict.update({key:form.get(key) for key in cols})
            mDict['upload_details_mixed_doc_uuid'] = form.get('upload_details').get('mixed_doc_uuid')
            mDict['upload_details_mixed_page_indexes'] = form.get('upload_details').get('mixed_doc_page_indexes')
            mDict['form_type'] = form.get('form_type').get('name')
            #mDict['confidence'] = form.get('upload_details').get('confidence')
            class_list.append(mDict)
            print('f')

    fDF = pd.DataFrame.from_records(class_list)
    fDF.to_csv(class_dir+'/classification.csv')



def flattenPaystubBase(lst,row):
    rDict=dict()
    # LOOP SCALAR KEYS
    scalars = [y for y in row.keys() if type(row.get(y)) not in (list,dict)]
    for y in scalars:
        rDict[y]=row[y]
    lst.append(rDict)

    flatDictLst=['employer','employee','employment_details','paystub_details']
    for x in flatDictLst:
        f = flattenDict(row.get(x))
        f = {x +'_' + str(key): val for key, val in f.items()}
        rDict.update(f)

    subkey = 'totals'
    nestedLst=['net_pay']
    for x in nestedLst:
        f = flattenDict(row.get(x).get(subkey))
        f = {x +'_' + str(key): val for key, val in f.items()}
        rDict.update(f)

    nestedLst=['earnings','deductions']
    for x in nestedLst:
        for i, dictVal in enumerate(row.get(x).get(subkey)):
            f = flattenDict(dictVal)
            f = {x +'_totals_' + str(key)+'_'+str(i): val for key, val in f.items()}
            rDict.update(f)

def flattenPaystubEarnings(lst,row):
    key1='earnings'
    baseDict={'uuid':row.get('uuid'),'book_uuid':row.get('book_uuid'),'doc_uuid':row.get('doc_uuid')}
    for x in row.get(key1).get('subtotals'):
        f = flattenDict(x)
        lst.append(baseDict|f)

def flattenPaystubDeductions(lst,row):
    key1='deductions'
    baseDict={'uuid':row.get('uuid'),'book_uuid':row.get('book_uuid'),'doc_uuid':row.get('doc_uuid')}
    for x in row.get(key1).get('subtotals'):
        f = flattenDict(x)
        lst.append(baseDict | f)

def flattenPaystub(prj_dir):
    ps_dir = prj_dir+'/outbound/Paystub/'
    df = getDirectoryHierarchy(ps_dir)
    myDF = df[df['file'].str.contains('bookPaystub')]

    paystub_base=list()
    paystub_earnings = list()
    paystub_deductions = list()

    for index, row in myDF.iterrows():
        with open(row.directory + '/' + row.file, 'r') as ps:
            ps_json = json.load(ps)

        for x in ps_json['response']:
            flattenPaystubBase(paystub_base,x)
            flattenPaystubEarnings(paystub_earnings,x)
            flattenPaystubDeductions(paystub_deductions,x)

    print('f')
    psDF = pd.DataFrame.from_records(paystub_base)
    psDF.to_csv(ps_dir+'/paystubBase.csv')
    psEarningsDF = pd.DataFrame.from_records(paystub_earnings)
    psEarningsDF.to_csv(ps_dir+'/paystubEarnings.csv')
    psDeductionsDF = pd.DataFrame.from_records(paystub_deductions)
    psDeductionsDF.to_csv(ps_dir+'/paystubDeductions.csv')

def flatten_risk_score(prj_dir,book_list):
    rs_dir = prj_dir+'/outbound/RiskScore/'
    df = getDirectoryHierarchy(rs_dir)
    rs_list = []
    for index, row in df.iterrows():
        with open(row.directory + '/' + row.file, 'r') as ds:
            ds_json = json.load(ds)
            bl = book_list.get('response')
            bl_entry = list(filter(lambda book: book['book_uuid'] == ds_json.get('book_uuid'), bl))[0]
            bl_dict={'book_name':bl_entry.get('name'), 'book_pk':bl_entry.get('pk')}
            fi_dict = flattenDict(ds_json)
            bl_dict.update(fi_dict)
            rs_list.append(bl_dict)
    rsDF = pd.DataFrame.from_records(rs_list)
    rsDF.to_csv(rs_dir+'/riskScore.csv',index=False)

def add_reason_code_columns(fa_dict):
    for i in range(9):
        fa_dict['reason_code_'+str(i)] = None
        fa_dict['reason_confidence_' + str(i)] = None
        fa_dict['reason_description_' + str(i)] = None
    return fa_dict


def flatten_detect_signals(prj_dir):
    cust_detect = prj_dir + '/outbound/Detect/'
    df = getDirectoryHierarchy(cust_detect)
    adf = df[df['file'].str.endswith('.json')]

    form_list=[]
    ### ITERATE ALL DETECT JSON
    for index,row in adf.iterrows():

        with open(row.directory + '/' + row.file, 'r') as ddf:
            dd_json = json.load(ddf)

        ### BOOK LEVEL METADATA
        book_name = row.application_name
        book_uuid = dd_json.get('book_uuid')
        book_dashboard_url = dd_json.get('book_dashboard_url')
        book_metadata_dict = {'book_name': book_name, 'book_uuid': book_uuid, 'book_dashboard_url': book_dashboard_url}

        for da in dd_json.get('doc_analysis'):
            ## DOC LEVEL METADATA
            doc_level_dict= book_metadata_dict.copy()
            for x in ['uploaded_doc_uuid', 'uploaded_doc_type', 'detect_status', 'mixed_uploaded_doc_uuid',
                      'is_image_based_pdf']:
                doc_level_dict[x] = da.get(x)

            ## FORM LEVEL METADATA
            for form_analysis in da.get('form_analysis'):
                form_signal_count = 0
                fa_dict =  doc_level_dict.copy()

                for x in ['form_type','form_uuid','form_dashboard_url']:
                    fa_dict[x] = form_analysis.get(x)
                fa_dict['form_signal_count'] = form_signal_count
                fa_dict['form_authenticity'] = form_analysis.get('form_authenticity').get('score')
                fa_dict = add_reason_code_columns(fa_dict)

                for count,value in enumerate(form_analysis.get('form_authenticity').get('reason_codes')):
                    fa_dict['reason_code_'+str(count)] = value.get('code')
                    fa_dict['reason_confidence_'+str(count)] = value.get('confidence')
                    fa_dict['reason_description_'+str(count)] = value.get('description')

                ## INTEROGATE SIGNAL ANALYSIS
                for sig in form_analysis.get('signals'):
                    identifier = sig.get('identifier')
                    signal_count = sig.get('signal_count')
                    form_signal_count+=signal_count
                    if identifier in fa_dict:
                        fa_dict[identifier] = fa_dict[identifier] + signal_count
                    else:
                        fa_dict[identifier] = signal_count
                fa_dict['form_signal_count'] = form_signal_count
                form_list.append(fa_dict)

    form_analysis_DF = pd.DataFrame.from_records(form_list)

    ### AGGREGATE BOOK LEVEL SIGNALS / COUNT
    book_cols = ['book_name', 'book_uuid', 'book_dashboard_url']
    groupby = form_analysis_DF.groupby('book_name')
    bookDF = groupby[book_cols].max().join(groupby[['form_authenticity']].min()).join(groupby.sum().drop(columns=['form_authenticity','is_image_based_pdf']))
    bookDF.rename(columns={'form_signal_count':'book_signal_count'},inplace=True)

    ### WRITE DATAFRAMES TO CSV
    form_analysis_DF.to_csv(cust_detect + 'FormDetectSignals.csv')
    bookDF.to_csv(cust_detect + 'BookDetectSignals.csv')
    print('End flatten_detect_signals')

