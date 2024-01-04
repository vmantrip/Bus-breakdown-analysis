import sqlite3
from sqlite3 import Error
import regex as re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):

    if drop_table_name:  # Can optionally pass drop_table_name to drop the table.
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)

    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows


def exec_many(conn, query, val):
    cur = conn.cursor()
    cur.executemany(query, val)
    conn.commit()


def get_columns_from_data_file(data_filename, *column_names):
    """Gets data specified columns from csv file as a generator (using yield).

    Args:
        data_filename (str): the name of the csv file.

    Yields:
        tuple: tuple of each row in the csv file.
    """
    with open(data_filename, 'r') as f:
        # first line is header
        header = f.readline().strip().split(",")
        header[0]='School_Year'
        #print(header)
        column_indices = [header.index(column_name)
                          for column_name in column_names]
        for row in f:
            row = row.strip()
            row = row.split("\"")
            appended_row = []
            for i in range(len(row)):
                if i % 2 == 1:
                    appended_row.append(row[i])
                elif len(row[i].strip()) > 0:
                    start_index = 0
                    end_index = len(row[i])
                    if row[i] == ",":
                        continue
                    if row[i][0] == ",":
                        start_index += 1
                    if row[i][-1] == ",":
                        end_index = -1
                    appended_row += row[i][start_index:end_index].split(",")
            row = appended_row
            selected_cols_in_row = [row[column_index]
                                    for column_index in column_indices]
            yield tuple(selected_cols_in_row)

def create_single_column_table(db_file, db_column_name, db_data_type="TEXT"):
    """
    Create a table for all the run types from the csv file.
    """
    with create_connection(db_file) as conn:
        create_table(conn, f"""CREATE TABLE IF NOT EXISTS {db_column_name} (
            {db_column_name}ID INTEGER NOT NULL PRIMARY KEY,
            {db_column_name} {db_data_type} NOT NULL UNIQUE)""")

def insert_into_single_column_table(db_file, db_column_name, column_data):
    with create_connection(db_file) as conn:
        column_data=list(sorted(column_data))
        exec_many(conn, f"INSERT or IGNORE INTO {db_column_name} ({db_column_name}) VALUES (?)", column_data)

def create_single_columns(db_file, data_file, column_name_data_type):
    column_names, db_data_types, format_func = zip(*column_name_data_type)
    BATCH_SIZE = 100000
    columns_generator = get_columns_from_data_file(data_file, *column_names)
    # create list of rows from the columns_generator from the specified batch size
    batch_columns = [x for _, x in zip(range(BATCH_SIZE), columns_generator)]
    create_table = True
    batch_index = 1
    while len(batch_columns) > 0:
        print(f"starting batch {batch_index}")
        columns = list(map(list, zip(*batch_columns)))
        for i in range(len(columns)):
            if create_table:
                create_single_column_table(db_file, column_names[i], db_data_types[i])
            column_data = format_func[i](columns[i])
            insert_into_single_column_table(db_file, column_names[i], column_data)
        batch_columns = [x for _, x in zip(range(BATCH_SIZE), columns_generator)]
        create_table = False
        print(f"completed batch {batch_index}")
        batch_index += 1

def format_string(column):
    """Converts to list of strings and removes redundant entries

    Args:
        column (list(str)): Input column to be converted

    Returns:
        list(str): converted column
    """
    return list(set(map(lambda c: (str(c),), column)))

def format_int(column):
    """Converts to list of strings to int

    Args:
        column (list(str)): Input column to be converted

    Returns:
        list(int): converted column
    """
    return list(map(lambda c: (int(c),), column))

def format_float(column):
    """Converts to list of strings to float

    Args:
        column (list(str)): Input column to be converted

    Returns:
        list(float): converted column
    """
    return list(map(lambda c: (float(c),), column))

# NOT COMPLETE!!!
def format_schools_serviced(column):
    '''
    OPT Codes of all transportation sites on the route. If there is more than one site,
    each site code will be separated by a comma. If the incident occurred on a bus used for Pre-K/EI service,
    the code will have one alpha, three numeric and sometimes one additional alpha character.
    If the incident occurred on a bus used for school-aged service,
    the code will have five text formatted numerals and may include a leading zero.
    '''
    new_list=[]
    for i in column:
        if not i.strip():
            continue
        else:
            try:
                number=int(i)
                if number<100000:
                    new_list.append(new_list)
                else:
                    continue
            except:
                continue
    return new_list


    

def get_single_column_name_data_type():
    """Gets the columns that are independent with information about DB type and conversion method

    Returns:
        list(tuple): 
    """
    return [   
        ("Run_Type", "TEXT", format_string),
        # ("Bus_No", "TEXT", format_default),
        ("Route_Number", "TEXT", format_string),
        ("Reason", "TEXT", format_string),
        # ("Schools_Serviced", "TEXT", format_schools_serviced),
        ("Boro", "TEXT", format_string),
        ("Bus_Company_Name", "TEXT", format_string),
        ("School_Age_or_PreK", "TEXT", format_string),
        ("Breakdown_or_Running_Late", "TEXT", format_string),
        ("School_Year","TEXT",format_string),
        ("Bus_No",'TEXT',format_string)

    ]

def get_single_column_dict(db_file, column_name):
    with create_connection(db_file) as conn:
        column_with_id = execute_sql_statement(f"SELECT {column_name}, {column_name}ID FROM {column_name}", conn)
        return dict(column_with_id)

def create_breakdown_table(db_file):
    with create_connection(db_file) as conn:
        create_table(conn, 
                     """create table if not exists Busbreakdown (
                        Busbreakdown_ID integer primary key,
                        Run_TypeID integer not null,
                        ReasonID integer not null,
                        BoroID integer not null,
                        Bus_Company_NameID integer not null,
                        Route_NumberID integer not null,
                        School_Age_or_PreKID integer not null,
                        Number_Of_Students_On_The_Bus integer not null,
                        Breakdown_or_Running_LateID INTEGER NOT NULL,
                        School_YearID INTEGER NOT NULL,
                        Bus_NoID Integer NOT NULL,
                        foreign key (Run_TypeID) references Run_Type(Run_TypeID),
                        foreign key (ReasonID) references Reason(ReasonID),
                        foreign key (BoroID) references Boro(BoroID),
                        foreign key (Bus_Company_NameID) references Bus_Company_Name(Bus_Company_NameID),
                        foreign key (Route_NumberID) references Route_Number(Route_NumberID),
                        foreign key (School_Age_or_PreKID) references School_Age_or_PreK(School_Age_or_PreKID),
                        foreign key (Breakdown_or_Running_LateID) references Breakdown_or_Running_Late(Breakdown_or_Running_LateID),
                        foreign key (School_YearID) references School_Year(School_YearID),
                        foreign key (Bus_NoID) references Bus_No(Bus_NoID))""")
        
def populate_breakdown_table(db_file, data_file):
    column_names = [
        "Busbreakdown_ID",
        "Run_Type",
        "Reason",
        "Boro",
        "Bus_Company_Name",
        "Route_Number",
        "School_Age_or_PreK",
        "Number_Of_Students_On_The_Bus",
        "Breakdown_or_Running_Late",
        "School_Year",
        "Bus_No"
        ]
    BATCH_SIZE = 100000
    columns_generator = get_columns_from_data_file(data_file, *column_names)
    batch_columns = [x for _, x in zip(range(BATCH_SIZE), columns_generator)]
    create_breakdown_table(db_file)
    batch_index = 1
    while len(batch_columns) > 0:
        print(f"starting batch {batch_index}")
        columns = list(map(list, zip(*batch_columns)))
        run_type_dict = get_single_column_dict(db_file, "Run_Type")
        reason_dict = get_single_column_dict(db_file, "Reason")
        boro_dict = get_single_column_dict(db_file, "Boro")
        bus_company_name_dict = get_single_column_dict(db_file, "Bus_Company_Name")
        route_number_dict = get_single_column_dict(db_file, "Route_Number")
        school_age_or_pre_k_dict = get_single_column_dict(db_file, "School_Age_or_PreK")
        breakdown_or_running_late_dict = get_single_column_dict(db_file, "Breakdown_or_Running_Late")
        school_year_dict=get_single_column_dict(db_file, "School_Year")
        bus_no_dict = get_single_column_dict(db_file, "Bus_No")
        def check_validity(break_down_id, run_type, reason, boro, bus_company_name, route_number, school_age_or_pre_k, number_of_students_on_the_bus,breakdown_or_running_late,school_year,bus_no):
            return run_type in run_type_dict\
                    and reason in reason_dict\
                    and boro in boro_dict\
                    and bus_company_name in bus_company_name_dict\
                    and route_number in route_number_dict\
                    and school_age_or_pre_k in school_age_or_pre_k_dict\
                    and breakdown_or_running_late in breakdown_or_running_late_dict\
                    and school_year in school_year_dict\
                    and bus_no in bus_no_dict
        def clean_up(break_down_id, run_type, reason, boro, bus_company_name, route_number, school_age_or_pre_k, number_of_students_on_the_bus,breakdown_or_running_late,school_year,bus_no):
            return (break_down_id, 
                    run_type_dict[run_type],
                    reason_dict[reason],
                    boro_dict[boro],
                    bus_company_name_dict[bus_company_name],
                    route_number_dict[route_number],
                    school_age_or_pre_k_dict[school_age_or_pre_k],
                    number_of_students_on_the_bus,
                    breakdown_or_running_late_dict[breakdown_or_running_late],
                    school_year_dict[school_year],
                    bus_no_dict[bus_no])
                    
        valid_rows = filter(lambda t: check_validity(*t), batch_columns)
        to_ids = map(lambda t: clean_up(*t), valid_rows)
        with create_connection(db_file) as conn:
            exec_many(conn, """INSERT INTO Busbreakdown (
                Busbreakdown_ID,
                Run_TypeID,
                ReasonID,
                BoroID,
                Bus_Company_NameID,
                Route_NumberID,
                School_Age_or_PreKID,
                Number_Of_Students_On_The_Bus,
                Breakdown_or_Running_LateID,
                School_YearID,
                Bus_NoID) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                to_ids)
        batch_columns = [x for _, x in zip(range(BATCH_SIZE), columns_generator)]
        print(f"completed batch {batch_index}")
        batch_index += 1

def get_bus_company_stats(db_file):
    """Queries list of bus company names sorted in descending order based on the number of won't start breakdown reason
    """
    with create_connection(db_file) as conn:
        data = execute_sql_statement("""
                              SELECT bcn.Bus_Company_Name,
            count(bbd.Bus_Company_NameID) as Failures
            FROM Busbreakdown AS bbd
            LEFT JOIN Run_Type AS rt ON rt.Run_TypeID = bbd.Run_TypeID
            LEFT JOIN Reason as reas on reas.ReasonID = bbd.ReasonID
            LEFT JOIN Bus_Company_Name as bcn on bcn.Bus_Company_NameID = bbd.Bus_Company_NameID
            LEFT JOIN Breakdown_or_Running_Late AS brs ON brs.Breakdown_or_Running_LateID = bbd.Breakdown_or_Running_LateID
            LEFT JOIN School_Year AS sy ON sy.School_YearID = bbd.School_YearID
            LEFT JOIN Bus_No AS bn ON bn.Bus_NoID = bbd.Bus_NoID
            WHERE reas.Reason = "Won`t Start"
            GROUP by bbd.Bus_Company_NameID
            order by Failures DESC""", conn)
    return data
    
db_file = "test.db"
data_file = "data_project.csv"
import os
if os.path.isfile(db_file):
    os.remove(db_file)
create_single_columns(db_file, data_file, get_single_column_name_data_type())
populate_breakdown_table(db_file, data_file)
#print(get_bus_company_stats(db_file))
count=0
sub_header=[]
sub_datalist=[]
with open(data_file, 'r') as f:
    for line in f:
        line=line.strip()
        x=line.split(',')
        if count==0:
            sub_header=[x[1],x[6]]
            count+=1
        else:
            try:
                number=int(x[6])
                if number<100000:
                    sub_datalist.append((x[1],number))
            except:
                continue

#print(sub_header)
#print(len(sub_datalist))


with create_connection(db_file) as conn:
    create_table(conn,"CREATE TABLE Schools_Serviced(Busbreakdown_ID TEXT NOT NULL,Schools_Serviced INTEGER NOT NULL);")
with create_connection(db_file) as conn:
        exec_many(conn,"INSERT INTO Schools_Serviced(Busbreakdown_ID,Schools_Serviced) VALUES(?,?)",sub_datalist)
with create_connection(db_file) as conn:
    final_data= execute_sql_statement("""
                              SELECT * FROM Busbreakdown
                              INNER JOIN Schools_Serviced AS ss ON ss.Busbreakdown_ID = Busbreakdown.Busbreakdown_ID
                              """, conn)
#print(len(final_data))
wrong_bus_number = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]
wrong_reason = [11]
wrong_boroID = [1,2]
wrong_company_name = [1,114,117,120,126,130,131,133]
new_lst = []
for i in final_data:
    new_lst.append([i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9],i[10],i[12]])
#print(new_lst)
clean_lst = []
for i in new_lst:
    if (i[0] in wrong_bus_number) or (i[2] in wrong_reason) or (i[3] in wrong_boroID) or (i[4] in wrong_company_name):
        continue
    else:
        clean_lst.append(i)
#print(clean_lst)
#print(len(clean_lst))
#print(final_data)
#final data tuples are BusbreakdownID 0,Run_TypeID 1,ReasonID 2,BoroID 3,Bus_CompanyID 4,Route_NumberID 5,School_Age_or_PreKID 6,Number_Of_Students_On_The_Bus 7,Breakdown_or_Running_LateID 8,School_YearID 9,Bus_NoID 10
#I am mentioning row numbers in respective tables!
#Bus noID to be removed: 1 to 15
#Reason ID 11
#BoroID 1,2
#Bus company names: 1,114,117,120,126,130,131,133
#Bus_No: 1 to 15





x = list(range(16,17465))
#we are checking for bus company_id 4, 
company_id_lst = []
for i in clean_lst:
    company_id_lst.append(i[4])

count_lst = []
for i in x:
    count_lst.append(company_id_lst.count(i))
#print(count_lst)
#print(len(count_lst))

zip_lst = list(zip(count_lst,x))

zip_lst = sorted(zip_lst)
#print(zip_lst)
update = []
for i in zip_lst:
    if i[0] ==0:
        continue
    else:
        update.append(i)
update = update[0:10]
#print(update)

x_axis = []
for i in update:
    x_axis.append(i[1])
#print(x_axis)


y_axis = []
for i in update:
    y_axis.append(i[0])

zippeds = list(zip(x_axis,y_axis))#[0] is company id and [1] is number of breakdown
#print(zippeds)


final_plot = []
bus_company_name_dict = get_single_column_dict(db_file, "Bus_Company_Name")
#print(bus_company_name_dict)
#for i in zippeds:
#    if i[0] in bus_company_name_dict:
#        final_plot.append(tuple(bus_company_name_dict[i[0]],i[1]))
    
#print(final_plot)

x_ele = []
y_ele = []

final_zipped = [['112',1],['111',2],['113',2],['119',3],['63',6],['127',8],['84',14],['86',15],['106',15],['101',19]]
for item in final_zipped:
    x_ele.append(item[0]) 
    y_ele.append(item[1])



plt.bar(x_ele, y_ele)
plt.xlabel('Bus Company ID')
plt.ylabel('Number of Breakdowns')
plt.title('Histogram of Number of Breakdowns  associated with company', fontweight ="bold")
plt.show()

x_3 = []
y_3 = []
top3 = [['R&C TRANSIT',1],['R&C TRANSIT B232',2],['SELBY CORP B2192',2]]
for item in top3:
    x_3.append(item[0]) 
    y_3.append(item[1])
  

plt.bar(x_3, y_3)
plt.xlabel('Bus Company ID')
plt.ylabel('Number of Breakdowns')
plt.title('Histogram of top 3 companies based on no. of breakdowns', fontweight ="bold")
#---------
plt.show()
#--------

zipp_plot= list(zip(*final_zipped))
#print(zipp_plot)
#plt.hist(*zip(*final_zipped))
#plt.xlabel('Bus Company ID')
#lt.ylabel('Number of Breakdowns')
#plt.title('Histogram of Number of Breakdowns  associated with company', fontweight ="bold")
#plt.show()
plt.scatter(*zip(*final_zipped))
li_fit =list(zip(*final_zipped))
plt.xlabel('Bus Company ID')
plt.ylabel('Number of Breakdowns')
plt.title('Histogram of Number of Breakdowns  associated with company', fontweight ="bold")
#print(li_fit)
#--------------
plt.show()
#---------

#fig,ax1 = plt.subplots(figsize=(10,10))
 #Creating histogram
#plt.hist(x_axis, bins = 10, label = 'x-axis')
#plt.hist(y_axis, bins = 10, label = 'y-axis')
 #Show plot
#plt.show()
#ax1.scatter(x_axis,y_axis)#plotting the actual graph
#ax1.ticklabel_format(style='plain')#change y axis to non scientific notation
#m,b = np.polyfit(x_axis,y_axis,1)#regression line
#plt.plot(x_axis,x_axis*x_axis+y_axis, color='red')
#plt.ticklabel_format(style='plain')
#plt.show()#display


#Breakdown_or_Running_LateID 8
#Bus_NoID 10


x = list(range(2,17138))
#print(count_lst)
#print(len(count_lst))

#print(zip_lst)
route_id_lst = []
for i in clean_lst:
    route_id_lst.append(i[5])
#print(route_id_lst)

count_lst = []
for i in x:
    number=route_id_lst.count(i)
    count_lst.append([number,i])
x=sorted(count_lst,reverse=True)
y=x[0:10]
#print(y)
z=[]
for i in y:
    z.append(i[0])
my_labels=['M617','M614','M604','M889','M966','M179','M605','M609','M659','M611']
my_explode=[0.2,0,0,0,0,0,0,0,0,0]
values = np.array(z)
plt.pie(z,labels=my_labels,autopct='%1.1f%%',explode=my_explode)
#plt.pie(values, labels = my_labels)
plt.title('Pie Chart of Route id vs number of breakdowns', fontweight ="bold")
plt.legend()
#----------
plt.show() 
#--------

#1st-Select route id
#2nd observe number of breakdowns over 8 yr period-one list
#[479, 4279], [425, 4276], [386, 4266] 0 is number of breakdowns, 1 is rote id
#what we need 
route_list=[4279,4276,4266]#5 is index in cleanlist
req_list=[]
for i in clean_lst:
    if i[5] in route_list:
        req_list.append(i)
#print(req_list)
x=list(range(1,9))
#print(x)
company1=[]
company2=[]
company3=[]
for i in req_list:
    if i[5]==4279:
        company1.append([i[9],i[5]])
    elif i[5]==4276:
        company2.append([i[9],i[5]])
    elif i[5]==4266:
        company3.append([i[9],i[5]])
    else:
        continue
comp1_sort=sorted(company1)
comp2_sort=sorted(company2)
comp3_sort=sorted(company3)
#print(comp1_sort)
#print(comp2_sort)
#print(comp3_sort)
year_id_list1=[]
for i in comp1_sort:
    year_id_list1.append(i[0])
x=list(range(1,9))
count_list1=[]
for i in x:
    count_list1.append(year_id_list1.count(i))
comp1_zip=list(zip(x,count_list1))
#print(comp1_zip)
#comp2
x=list(range(1,9))
year_id_list2=[]
for i in comp2_sort:
    year_id_list2.append(i[0])
count_list2=[]
for i in x:
    count_list2.append(year_id_list2.count(i))
comp2_zip=list(zip(x,count_list2))
#print(comp2_zip)
#comp3
year3=[]
x=list(range(1,9))
year_id_list3=[]
for i in comp3_sort:
    year_id_list3.append(i[0])
count_list3=[]
for i in x:
    count_list3.append(year_id_list3.count(i))
comp3_zip=list(zip(x,count_list3))
#print(comp3_zip)
x1=['2015','2016','2017','2018','2019','2020','2021','2022']
y1=[x[1] for x in comp1_zip]
plt.plot(x1,y1)
plt.plot(x1,y1,'or')
plt.xlabel('Year')
plt.ylabel('Number of Breakdowns')
plt.title('Trend observed in route "M617"', fontweight ="bold")
plt.show()
x2=['2015','2016','2017','2018','2019','2020','2021','2022']
y2=[x[1] for x in comp2_zip]
plt.plot(x2,y2)
plt.plot(x2,y2,'or')
plt.xlabel('Year')
plt.ylabel('Number of Breakdowns')
plt.title('Trend observed in route "M614"', fontweight ="bold")
plt.show()
x3=['2015','2016','2017','2018','2019','2020','2021','2022']
y3=[x[1] for x in comp3_zip]
plt.plot(x3,y3)
plt.plot(x3,y3,'or',label='')
plt.xlabel('Year')
plt.ylabel('Number of Breakdowns')
plt.title('Trend observed in route "M604"', fontweight ="bold")
plt.show()

plt.plot(x1,y1,)
plt.plot(x1,y1,'r',label='M617')
plt.plot(x2,y2)
plt.plot(x2,y2,'b',label='M614')
plt.plot(x3,y3)
plt.plot(x3,y3,'g',label='M604')
plt.xlabel('Year')
plt.ylabel('Number of Breakdowns')
plt.title('Breakdown trends in top3 most affected in top 3 routes', fontweight ="bold")
plt.legend()
plt.show()