# **************************
# CVML2 Data insert model.
# **************************

# Import necessary libraries...
import datetime
import pyodbc

# ProcessMaster table column names...

"""('EBTBRICKJAM',),('EBTMASSFilling',),('EBTLevelling',),('HMPositioning',),('HMPouring',),('LaunderInsertion',),('TopLancePositioning',),('ScrapCharging',),('Gunning',),('Fettling',),
('TopRoofRemoval',),('SlagDoorCleaning',),('TopRoofPutBack',),('LaunderPouringBlowing',),('LaunderPouringArcing',),('TopPouringBlowing',),('TopPouringArcing',)  """

# Connect CVML2 DB...
print("Try to connect SQL Server for cvmlautomation2")

# Give sql credentials...
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                  'Server=172.21.25.164;'
                  'Database=cvmlautomation2;'
                  'UID=sa;'
                  'PWD=admin@123;'
                  'MARS_connection=yes')

cursor = conn.cursor()
print("Connection successful for cvmlautomation2")

# Connect HeatinfoDB...
print("Try to connect SQL Server for HeatInfoDb")

# Give sql credentials...
conn1 = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                  'Server=172.21.25.164;'
                  'Database=HeatInfoDb;'
                  'UID=sa;'
                  'PWD=admin@123;'
                  'MARS_connection=yes')

cursor1 = conn1.cursor()
print("Connection successful for HeatInfoDb")

# Print all column name from ProcessMaster...
# cursor.execute("select * from ProcessMaster")
# result = cursor.fetchall()
# num_fields = len(cursor.description)
# field_names = [i[0] for i in cursor.description]
# print(field_names)

# Value required for insert to SQL DB...
"""
# Column name
    # (Procseq, IsEnable, IsActive, liveProcessId, StartTime, EndTime, MstProcessId, Duration, SopTime, ShellNo,heatno)

    # Value
    #(1,'true','true',0,'2020-01-31 09:51:55.000','2020-01-31 09:56:04.000',4,0,100,1,200100444)

    1.Procseq - 1 (Procseq3)
    2.IsEnable - 'true'(IsEnable5)
    3.IsActive - 'true'(IsActive6)
    4.liveProcessId - 0 (ProcessId4)
    5.StartTime - '2020-01-31 09:51:55.000'
    6.EndTime - '2020-01-31 09:56:04.000'
    7.MstProcessId - 4 (MstProcessId0)
    8.Duration - EndTime - StartTime
    9.SopTime - 100 (SOP11)
    10.ShellNo - 1
    11.heatno - 200100444
"""

# *************************************************************************************************************************************************
# This function will help to insert data for CVMLAutomation2...

def insert_data_EBT_Mass_Filling(my_dict):

    # Taking key from dict where model will pass... (Process name)
    key = list(my_dict.keys())
    processname = key[0]
    # print("Process name-{}".format(processname))

    # Taking value from dict where model will pass...
    values = list(my_dict.values())
    stime = values[0][0]
    etime = values[0][1]
    shellN = values[0][2]

    # This query will get data from ProcessMaster table...
    query="SELECT * FROM CVMLAutomation2.dbo.ProcessMaster WHERE ProcessAlias = '{}'".format(processname)
    # print(query)

    try:

        # Checking SQL connection...
        if cursor.connection:

            # Assign query...
            process_select_query = query

            # EXECUTE QUERY...
            cursor.execute(process_select_query)

            # Fetch all data and save into myresult variable...
            myresult = cursor.fetchall()

            # Create a empty list for append data...
            datalist = []
            for a in myresult:

                # Append value to datalist...
                datalist.append(a)

            # Checking SQL connection for HeatInfoDB...
            if cursor1.connection:

                # Max value query from tbLL2HeatInfo...
                heatno_query = "SELECT max (HEAT_NAME) FROM [HeatInfoDb].[dbo].[tblL2HeatInfo]"

                # Execute query...
                cursor1.execute(heatno_query)

                # Getting max value...
                heatno = cursor1.fetchall()
                # print("heatno-{}".format(heatno[0][0]))

            # Define a data class to store value...
            class data:

                # Define objects...
                def __init__ (self, Procseq, IsEnable,IsActive,liveProcessId,StartTime,EndTime,MstProcessId,Duration,SopTime,ShellNo,heatno):

                    # Assign value to object...
                    self.Procseq = Procseq
                    self.IsEnable = IsEnable
                    self.IsActive = IsActive
                    self.liveProcessId = liveProcessId
                    self.StartTime = StartTime
                    self.EndTime = EndTime
                    self.MstProcessId = MstProcessId
                    self.Duration = Duration
                    self.SopTime = SopTime
                    self.ShellNo = ShellNo
                    self.heatno = heatno

            # Create a variable and assign value to class object...
            sqldata = data(datalist[0][3],datalist[0][5],datalist[0][6],datalist[0][4],stime,etime,datalist[0][0],0,datalist[0][11],shellN,heatno[0][0])

            # print(sqldata.Procseq,sqldata.IsEnable,sqldata.IsActive,sqldata.liveProcessId,sqldata.StartTime,
            #       sqldata.EndTime,sqldata.MstProcessId,sqldata.Duration,sqldata.SopTime,sqldata.ShellNo,sqldata.heatno)

            # Data inseert query...
            Data_insert_Query = "INSERT INTO liveSubProcess(Procseq, IsEnable,IsActive,liveProcessId,StartTime,EndTime,MstProcessId,Duration,SopTime,ShellNo,heatno) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');".\
                format(sqldata.Procseq,sqldata.IsEnable,sqldata.IsActive,sqldata.liveProcessId,sqldata.StartTime,sqldata.EndTime,sqldata.MstProcessId,sqldata.Duration,sqldata.SopTime,sqldata.ShellNo,sqldata.heatno)

            # Checking the sql inserting data
            print(" Data Insert query {}".format(Data_insert_Query))

            # Insert query Code is execute...
        #     cursor.execute(Data_insert_Query)
        #     conn.commit()
        #
        #     # After execute code server will close...
        #     cursor.close()
        #     conn.close()
        #     print("Data has been inserted")
        #
        # else:
        #
        #     cursor.close()
        #     conn.close()

    except Exception as e:
        print("SQL connect failed for insert", e)


my_dict ={"EBTLevelling":('2021-07-28 12:28:12:000','2021-07-28 12:28:12',2)}

insert_data_EBT_Mass_Filling(my_dict)