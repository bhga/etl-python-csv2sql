import csv
import pandas as pd
import numpy as np
import MySQLdb
from datetime import date


inputpath=input("Enter the input path file:")

outputpath=input("Enter the output path file:")
outputpath = outputpath + '//' + str(date.today()) + '//Output.csv'

rejectpath=input("Enter the reject path file:")
rejectpath = rejectpath + '//' + str(date.today()) + '//Reject.csv'

dlm=input("Enter delimiter(must be a 1-character string):") #e.g ';','|',','

df =pd.read_csv(inputpath,sep=dlm)
print("Successfully read the input file")

MandatoryCheck = (df['col1'].isnull()) #variable used to store filter condition

validRecord = df.loc[~MandatoryCheck]   #Using tilde to reject filter condition 
invalidRecord = df.loc[MandatoryCheck]

validRecord.loc[:,'col2'] = validRecord['col2'].replace({np.NaN:'NA'}) #using numpy to match null values and replace it with NA

ConditionalCheck = (df['col2'].isnull())

validRecord.loc[~ConditionalCheck,'col2'] = validRecord['col2'] + validRecord['col3'] #if not null do the concatenation

validRecord.loc[:,'col5'] = date.today() #add new column with system date

validRecord.to_csv(outputpath,sep=dlm,index=False)
invalidRecord.to_csv(rejectpath,sep=dlm,index=False)
print("Successfully created the output file!!!")

db=MySQLdb.connect(host='',user='',password='',database='')
mycursor=db.cursor()

with open(outputpath,'r') as generatedfile:
    dbfile=csv.reader(generatedfile,delimiter=dlm)
    next(dbfile)
    emp_detail=tuple(dbfile)

query = 'insert into tablename (col1,col2,col3,col4,col5) values(%s,%s,%s,%s,%s)'

mycursor.executemany(query,emp_detail)
db.commit()
print("Import of the output file into MySQL is succesfull!!!")
