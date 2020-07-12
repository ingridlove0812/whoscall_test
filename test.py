# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 12:48:00 2020

@author: lailai_tvbs
"""

import os
from db_connect import connect_sql_gcp
import pandas as pd
import math
import itertools
import numpy as np
import re
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules



#驗證資料庫是否有資料
def valid_data(table_name):
    global db_name
    conn, cur = connect_sql_gcp(db_name)
    cur.execute("SELECT count(*) FROM %s.%s" % (db_name, table_name))
    cnt = cur.fetchone()
    cur.close()
    conn.close()
    return cnt[0]


#刪除舊有的資料
def delete_data(table_name):
    global db_name
    conn, cur = connect_sql_gcp(db_name)
    cur.execute("LOCK TABLES %s.%s WRITE" % (db_name, table_name))
    cur.execute("START TRANSACTION")
    cur.execute("DELETE FROM %s.%s" % (db_name, table_name))
    cur.execute("UNLOCK TABLES")
    conn.commit()
    cur.close()
    conn.close()


#將資料塞回DB
def insert_data(table_name, data):
    global db_name
    #分批塞入;每批100000筆
    batch_row = 100000
    conn, cur = connect_sql_gcp(db_name)
    colname = data.columns.tolist()
    cols = "`,`".join([str(i) for i in colname]).replace('.','')
    insert = "REPLACE INTO " + db_name + '.' + table_name + " (`" +cols + "`) VALUES (" + "%s,"*(len(tuple(data))-1) + "%s)"
    #如果資料筆數不超過100000筆就一次塞，不然分批
    if len(data) > batch_row:
        for i in range(0,math.ceil(len(data)/batch_row)):
            tmp_data = []
            for i,row in data[i*batch_row:(i+1)*batch_row-1].iterrows():
                tmp_data.append(tuple(row))
            cur.executemany(insert, tmp_data)
            conn.commit()
    else:
        tmp_data = []
        for i,row in data[:100].iterrows():
            tmp_data.append(tuple(row))
        cur.executemany(insert, tmp_data)
        conn.commit()
    cur.close()
    conn.close()


#從DB拉資料
def pull_data(table_name, string = 'null', integer = 'null'):
    global db_name
    conn, cur = connect_sql_gcp(db_name)
    cur.execute('SELECT * FROM %s.%s' % (db_name, table_name))
    cols = [i[0] for i in cur.description]
    tmp_data = cur.fetchall()
    tmp_data = pd.DataFrame(tmp_data, columns = cols)
    #將文字轉成utf8碼
#    if string != 'null':
#        tmp_data[string] = tmp_data[string].select_dtypes([np.object]).stack().str.decode('utf-8').unstack()
    #將有些應該是數字的欄位修改成int
#    if integer != 'null':
#        for l in integer:
#            tmp_data[l] = tmp_data[l].fillna(0).replace('',0).astype(int)
    cur.close()
    conn.close()
    return tmp_data


if __name__ == '__main__':
    os.chdir(os.getcwd())
    db_name = 'whoscall_test'
    data_1 = pd.read_csv("row_data.csv")
    #正常來說應該要先delete再insert，不過這裡因為資料不會持續更新，所以就單驗證資料庫裡是否有資料，沒有的話再驗證
    #delete_data('row_data')
    if valid_data('row_data') == 0:
        insert_data('row_data', data_1)
    data1_insert = pull_data('row_data', string = 'null', integer = 'null')
    data1_backup = data1_insert
    data1_insert['date'] = data1_insert['ts'].apply(lambda x:x.date())
    data1_user_cnt = data1_insert.groupby('date').agg({'id': pd.Series.nunique})
    data1_insert.groupby('id').agg({'date': pd.Series.nunique})
    test = data1_insert[['id','date']].drop_duplicates().sort_values(by=['id', 'date'])
    user_id = []
    days = []
    for i in set(test['id']):
        print(i)
        user_id.append([i]*len(list(set(test[test['id'] == i].groupby((~(test[test['id'] == i]['date'].diff() ==  pd.Timedelta(1, unit='d'))).astype(int).cumsum()).transform(len).iloc[:, 0]))))
        days.append(list(set(test[test['id'] == i].groupby((~(test[test['id'] == i]['date'].diff() ==  pd.Timedelta(1, unit='d'))).astype(int).cumsum()).transform(len).iloc[:, 0])))
    user_list  = list(itertools.chain(*user_id))
    days_list  = list(itertools.chain(*days))
    data1_days = pd.DataFrame(list(zip(user_list, days_list)), columns =['user', 'days'])
    days_2 = str(len(data1_days[data1_days['days'] == 2])/len(set(test['id'])) *100) + '%'
    days_5 = str(len(data1_days[data1_days['days'] == 5])/len(set(test['id'])) *100) + '%'
    days_7 = str(len(data1_days[data1_days['days'] == 7])/len(set(test['id'])) *100) + '%'
    days_14 = str(len(data1_days[data1_days['days'] == 14])/len(set(test['id'])) *100) + '%'
    days_30 = str(len(data1_days[data1_days['days'] == 30])/len(set(test['id'])) *100) + '%'


    data_2 = pd.read_csv("air_pollution.csv")
    if valid_data('air_pollution') == 0:
        insert_data('air_pollution', data_2)
    for i in data_2['No']:
        data_2.loc[data_2['No']==i,'avg'] = data_2[(data_2['No']>=i-2) & (data_2['No']<=i-1)]['pm2.5'].mean()
        data_2.loc[data_2['No']==i,'std'] = data_2[(data_2['No']>=i-2) & (data_2['No']<=i-1)]['pm2.5'].std()
    data_2.loc[data_2['pm2.5'] > data_2['avg']+3*data_2['std'],'warn_ind'] = 1
    data_2.loc[data_2['pm2.5'] <= data_2['avg']+3*data_2['std'],'warn_ind'] = 0
    data_2 = data_2[(data_2['warn_ind'] == 1) & (data_2['month'] <= 3)][['year','month','day','hour','pm2.5']].sort_values(by=['year','month','day','pm2.5'])
    data_2['rnk'] = data_2.groupby('year')['pm2.5'].rank(method='min',ascending=False)
    data_2_output = data_2[(data_2['rnk'] >=10) & (data_2['rnk'] <=20)][['year','month','day','hour','pm2.5','rnk']].sort_values(by=['year','month','day','hour'])


    data_3 = pd.read_excel("online_retail.xlsx")
    data_3['date'] = data_3['InvoiceDate'].apply(lambda x:x.date())
    data_3_1 = data_3.groupby(['CustomerID','StockCode']).agg({'date': pd.Series.nunique, 'Quantity':pd.Series.sum, 'UnitPrice':pd.Series.sum}).reset_index()
    data_3_1[(data_3_1['Quantity'] == 0) & (data_3_1['StockCode'] > 2)]
    data_3_1 = data_3.groupby(['StockCode']).agg({'CustomerID': pd.Series.nunique}).reset_index()
    data3_items = pd.pivot_table(data_3, values = ['Quantity'], index=['CustomerID'], columns=['StockCode'], aggfunc= lambda x: len(x.unique())).reset_index()
    data3_dates = pd.pivot_table(data_3, values = ['Quantity'], index=['CustomerID'], columns=['date'], aggfunc= lambda x: len(x.unique())).reset_index()
    data3_train = pd.merge(data3_items, data3_dates, on  = 'CustomerID')
    data3_train = data3_train.drop(['CustomerID'], axis=1)
    data3_train = data3_train>1
    data3_items = data3_items.drop(['CustomerID'], axis=1)
    data3_items = data3_items>1
    data3_itemsets = apriori(data3_items, min_support = 0.01, use_colnames=True)
    rules = association_rules(data3_itemsets, min_threshold=0.8)


    data3_no = pd.pivot_table(data_3, values = ['Quantity'], index=['InvoiceNo'], columns=['StockCode'], aggfunc= lambda x: len(x.unique())).reset_index()
    data3_no = data3_no.drop(['InvoiceNo'], axis=1)
    data3_no = data3_no>1
    data3_no_1 = apriori(data3_no, min_support = 0.001, use_colnames=True)
    rules = association_rules(data3_no_1, min_threshold = 0.1)

#    data = data_1
#    table_name = 'row_data'
