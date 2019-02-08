# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 15:59:23 2019

@author: vavashishtha
this is foaasdr DATAtrek
"""


import sys
import cx_Oracle
import pandas as pd

file_name='source_data.xlsx'#sys.argv[1]
sheets_name='Sheet1'#sys.argv[2]
Mapping_name='Mapping.xlsx'

df_excel = pd.read_excel(file_name, sheet_name=sheets_name)
df_Mapping=pd.read_excel(Mapping_name, sheet_name=sheets_name)

dic_excel=df_excel.to_dict('series')
lol = df_excel.values.tolist()
Excel_column_list=list(df_excel)
Mapping_column_list=df_Mapping['Excel_Column_Name'].tolist()
Table_Column_Name=df_Mapping['Table_Column_Name'].tolist()

def printf (format,*args):
    sys.stdout.write (format % args)

def printException (exception):
  error, = exception.args
  printf ("Error code = %s\n",error.code);
  printf ("Error message = %s\n",error.message);
  
def CompareTwoList(List1,List2):
  list_index=[]
  for item1 in List2:
    for item2 in List1:
        if (item1==item2):
            list_index.append(List1.index(item2)+1)

  return list_index

ls_index=CompareTwoList(Excel_column_list,Mapping_column_list)
  
  
#username = 'scott'
#password = 'tiger'
databaseName = "DATATREK"
print('vaibhav')
conn_str = 'etlapp/etlappstg@lvsspldb16.qa.paypal.com:2127/QADBAA9O'
try:
    conn = cx_Oracle.connect(conn_str)
except cx_Oracle.DatabaseError as exception:
  printf ('Failed to connect to %s\n',databaseName)
  printException (exception)
  exit(0)   

c = conn.cursor()
sql = """select * from Alti_PayPal_Mapping"""


Sql_execute="insert into Alti_PayPal_Mapping ("
for j in Table_Column_Name:
    Sql_execute=Sql_execute+str(j)+', '
Sql_execute = Sql_execute[:-2]    
Sql_execute=Sql_execute+' ) values ('
for i in ls_index:
    Sql_execute=Sql_execute+':'+str(i)+', '
Sql_execute = Sql_execute[:-2]
Sql_execute=Sql_execute+' )'

print(Sql_execute)    
    
  
c.executemany(Sql_execute, lol, batcherrors = True)
for errorObj in c.getbatcherrors():
    print("Row", errorObj.offset, "has error", errorObj.message)






#try:
  #  print(conn.version.split("."))
   # c.execute(sql)

#except cx_Oracle.DatabaseError as exception:
 # printf ('Failed to select from Alti_PayPal_Mapping\n')
 # printException (exception)
 # exit(0)
  
#result=c.fetchall()
#for row in result:
#  printf (" %s;%s;%s\n",row[0],row[1],row[2])
conn.commit()
conn.close()


