# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 15:59:23 2019

@author: vavashishtha
This Utility reads a csv data file and a csv mapping document and loads data into a table by using these two inputs.

Arguments Required in given order : [1] Source_data csv file, [2] Target_Table_name, [3] Mapping Document in csv format
Specifications for Target_Table_name : Please mention file name with proper schema 
Python calling Script example : Python Connection_dev.py 'Source_data.csv' 'Schema_name.Table_name' 'Mapping_Docuemnt.csv'
"""


import sys
import cx_Oracle 
import pandas as pd
import datetime

file_name = sys.argv[1]
Target_Table_name = sys.argv[2]
Mapping_name= sys.argv[3]


now = datetime.datetime.now()
Prepared_date=str(now.strftime("%Y%m%d"))+'0001'
df_excel = pd.read_csv(file_name,keep_default_na=False)
df_Mapping=pd.read_csv(Mapping_name)
df_excel['Extract_ID']=Prepared_date
df_excel['Row_Insert_TS']=now
df_excel['NaN']=float('NAN')
df_excel['Source_Row_Seq'] = range(1, 1+len(df_excel))
Excel_column_list=list(df_excel)
Source_Column_Name=df_Mapping['Source_Column_Name'].tolist()
Table_Column_Name=df_Mapping['Table_Column_Name'].tolist()

def printf (format,*args):
    sys.stdout.write (format % args)

def printException (exception):
  error, = exception.args
  printf ("Error code = %s\n",error.code);
  printf ("Error message = %s\n",error.message);

def CompareTwoList(List1,List2):
  list_index=[]
  flag_available=1
  for item1 in List2:
    flag_available=1
    for item2 in List1:
        if (item1==item2):
            list_index.append(List1.index(item2))
            flag_available=0
    if (flag_available==1):
        list_index.append(-1)

  return list_index


ls_index=CompareTwoList(Excel_column_list,Source_Column_Name)


ls_cols = df_excel.columns.tolist()
ls_col_list=[]

for j in ls_index:
    if(j==-1):
        ls_col_list.append('NaN')
    else:
        ls_col_list.append(ls_cols[j])

ls_col_list.append('Row_Insert_TS')

ls_col_list.append('Row_Insert_TS')

ls_col_list.append('Row_Insert_TS')

ls_col_list.append('Source_Row_Seq')

ls_col_list.append('Extract_ID')    ##added
df_excel = df_excel[ls_col_list]


Sql_execute="insert into "+Target_Table_name+ " ("
for j in Table_Column_Name:
    Sql_execute=Sql_execute+str(j)+', '

Sql_execute=Sql_execute+'FISCAL_PERIOD,ROW_CREATED_TIMESTAMP,ROW_UPDATED_TIMESTAMP,SOURCE_ROW_SEQUENCE_NUMBER,Extract_ID ) values ('
for i in ls_index:
    Sql_execute=Sql_execute+':'+str(i)+', '

Sql_execute=Sql_execute+':Row_Insert_TS,:Row_Insert_TS,:Row_Insert_TS,:Source_Row_Seq,:Extract_ID)'#+str(max(ls_index)+1)+' )'#', :'+str(max(ls_index)+2)+', :'+str(max(ls_index)+3)+' )'

databaseName = "DATATREK"
#print('vaibhav')
conn_str = 'etlapp/etlappstg@stage2cog02'
try:
    conn = cx_Oracle.connect(conn_str)
except cx_Oracle.DatabaseError as exception:
  printf ('Failed to connect to %s\n',databaseName)
  printException (exception)
  exit(0)

final_insert = df_excel.values.tolist()
c = conn.cursor()
sql_truncate = "DELETE FROM  "+Target_Table_name
sql_demo = """insert into Demo_table (SR_NO, FIRST_NAME, LAST_NAME, PP_NAME ) values (:0, :1, :2 ,:3)"""

print(Sql_execute)
#print(type(df_excel1))

print('Script will start Truncate the table '+Target_Table_name+' now')
c.execute(sql_truncate)
for errorObj in c.getbatcherrors():
    print("Row", errorObj.offset, "has error", errorObj.message)


print("Insertion will start Now,")
try:
   c.executemany(Sql_execute, final_insert, batcherrors = True)
except cx_Oracle.DatabaseError as exception:
  printf ('Failed to Insert into'+Target_Table_name)
  printException (exception)


for errorObj in c.getbatcherrors():
    print("Row", errorObj.offset, "has error", errorObj.message)

print('Insertion has Ended')


conn.commit()
conn.close()