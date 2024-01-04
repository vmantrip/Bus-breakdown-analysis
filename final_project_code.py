filename=open('data_project.csv','r')
count=0
details_list=[]# We need to clean this list!
for line in filename:
    if count==0:
        line=line.strip()
        line=line.strip('ï»¿')
        header=line.split(',')
    else:
        line=line.strip()
        line=line.strip('ï»¿')
        details_list.append(line.split(','))
    count+=1
#print(count)
#print(header)
#print(header[6])
#print(len(header))
#print(header[0])
#print(len(details_list))
#Columns of interest:
# School year - Split from and to- int 0 
# Busbreakdown_ID int/str 1 DONE
# Run_Type str 2 DONE
# Bus_No - str 3
# Route_Number -str 4 DONE
# Reason - str 5 DONE
# Schools_Serviced - Integer <100,000 6
# Boro - str 9 DONE
# Bus_Company_Name - str 10 DONE
# How_Long_Delayed int + str(min) Convert hrs to min! 11
# Number of students on the bus -  int 12 DONE
# Columns to build a score of responsibility: Has_Contractor_Notified_Schools 13 and Has_Contractor_Notified_Parents 14 - str 
# Breakdown_or_Running_Late - str 19
# School_Age_or_PreK - str 20 DONE

#------BEGINNING CLEANING PROCESS---------
# LEFTOVER COLUMNS
#  School year - Split from and to- int 0 
#  Bus_No - str 3
# Schools_Serviced - Integer <100,000 6
# How_Long_Delayed int + str(min) Convert hrs to min! 11
# Breakdown_or_Running_Late - str 19




from_to_years=[]
schools_service_list=[]
purify1_list=[]
for i in details_list:
    if i[0]!=None and i[1]!=None and i[2]!=None and i[3]!=None and i[4]!=None and i[5]!=None and i[6]!=None and i[9]!=None and i[10]!=None and i[11]!=None and i[12]!=None and i[13]!=None and i[14]!=None and i[19]!=None and i[20]!=None:
        try:
            schools_service_num=int(i[6])
            if schools_service_num<100000:
                schools_service_list.append(schools_service_num)
                from_to_years.append(i[0].split('-'))
                purify1_list.append(i)
        except:
            #print(i[6])
            continue

#Number of schools serviced and years have been checked
#print(purify1_list)
print(len(from_to_years))
print(len(schools_service_list))
print(len(purify1_list))

with create_connection(db_file) as conn:
        column_with_id = execute_sql_statement(f"SELECT {column_name}, {column_name}ID FROM {column_name}", conn)
        return dict(column_with_id)


