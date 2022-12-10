import csv
import MySQLdb
from datetime import date


inputpath=input("Enter the input path file:")

outputpath=input("Enter the output path file:")

dlm=input("Enter delimiter(must be a 1-character string):") #e.g ';','|',','

with open(inputpath,'r') as file:
    csvreader = csv.DictReader(file,delimiter=dlm)
    print("Succesfully read the input file!!!")


    with open(outputpath,'w',newline='') as outfile:
        columnname=['col1','col2','col3','col4','col5']
        csvwriter=csv.DictWriter(outfile,fieldnames=columnname,delimiter=dlm)
        csvwriter.writeheader()
        for r in csvreader:
            if any(field.strip() for field in r):
                if (r["col1"]!=''):
                    if (r["col2"]==''):
                        r["col2"]="NA"
                    else:
                        r["col2"]=r["col2"]+r["col3"]
                    r.update({"col5":date.today()})
                    csvwriter.writerow(r)

print("Succesfully created the output file!!!")
db=MySQLdb.connect(host='',user='',password='',database='')

with open(outputpath,'r') as generatedfile:
    dbfile=csv.reader(generatedfile,delimiter=dlm)
    next(dbfile)
    emp_detail=[]
    for r in dbfile:
        emp_detail.append(r)

query='insert into `tableName` (`col1`,`col2`,`col3`,`col4`,`col5`) values (%s,%s,%s,%s,%s)'

mycursor=db.cursor()
mycursor.executemany(query,emp_detail)
db.commit()
print("Import of the output file into MySQL is succesfull!!!")
