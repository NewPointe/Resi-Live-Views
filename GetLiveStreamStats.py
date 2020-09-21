import requests
import datetime
import time
import json
import csv
import pyodbc 
import logging
import creds
from convertTimestamp import convertTS
import pyfiglet
from user_agents import parse
import sendTeamsAlert as sta

open_banner = pyfiglet.figlet_format("NPO STREAM DUMP")
close_banner = pyfiglet.figlet_format("SUCCESS!")

logging.basicConfig(level=logging.DEBUG, filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

timestr = time.strftime("%Y%m%d")

conn = creds.conn
cursor = conn.cursor()

#Resi Login variables
login_url = creds.login_url
login_body = creds.login_body

username = creds.user
password = creds.password

#Function to log in and create a new user session.
#8/31/2020 - working
def login():
    with requests.Session() as s:

        r = s.post('https://central.resi.io/api/v3/login', json={"userName":username,"password":password})

        #Set the customer ID:
        r_response = r.json()
        customer_id = r_response['customerId']
        
        webevent_req = s.get(f'https://central.resi.io/api/v3/customers/2fe9d65c-7f27-46b7-95e1-bb7dfd234cc7/webevents')

        webevent_resp = webevent_req.json()

        global eventnamedict
        eventnamedict = {h['uuid']:h['name'] for h in webevent_resp}


    return s, webevent_resp
    

    
    

#I wrote a nifty function to check how many items are in a CSV url. 
def checkCSVLength(url, sesh):
    response = sesh.get(url)
    data = response.json()
    return len(data)

#I wrote a function that gets the url of the next page
def getCSVPaginationURL(url, url2, sesh):
    response = sesh.get(url2)
    data = response.json()
    print('hit pagination function')
    return url + '&offset=' + data[499]['clientId']


#Strips the list of web events down to a dict with event name and csv url.
def getCSVURLS(web_events, sesh):
    #create some temporary lists to hold data
    key_list = []
    val_list = []

    #debub
    print(sesh)

    #Let's take the lists with event names and csv download urls and save them in a dictionary.
    

    for obj in web_events:
        
        counter = 2
        if 'NewPointe Online' in obj['name']:
            print(obj['name'])
            print(obj['uuid'])
            #set the initial URL
            url = 'https://central.resi.io/api/v3/customers/2fe9d65c-7f27-46b7-95e1-bb7dfd234cc7/webevents/'+str(obj['uuid']) + '/export?max=500'

            #Let's see how long the first page is and store it in a variable
            page_len = checkCSVLength(url, sesh)

            #Store the inital values
            key_list.append(obj['name'])
            val_list.append(url)

            if page_len <500:
                print("less than 500")
                pass
            elif page_len == 500:

                #send a request to get the length of the data
                #Resi maxes out at 500
                url_var = url
                ctrl = 1
            
                while ctrl == 1:

                    new_url = getCSVPaginationURL(url, url_var, sesh)

                    url_var = new_url

                    key_list.append(obj['name']+str(counter))
                    val_list.append(url_var)

                    csv_length = checkCSVLength(url_var, sesh)
                    counter +=1

                    if csv_length < 500:
                        ctrl += 1 
    
    print(key_list)
    print(val_list)                 

        
    event_pages = {key_list[i]: val_list[i] for i in range(len(key_list))}
    
    print(event_pages)
    print("Step 3 Complete")

    return event_pages



#Sends a request to the csv url for an event and returns the JSON object and it's length
def getCSVData(expurl):
    response = sesh.get(expurl)
    statdata = response.json()
    length_data = len(statdata) - 1
    return statdata, length_data

#Inserts the data in a SQL table
#9/13/20 I don't think I actually need this function - utilizing parameterized queries instead***
'''def insertSql(headerdata, rowdata):
    

    #format the header so SQL accepts it:
    header =  ", ".join(['\'{}\''.format(x) for x in headerdata]) + "  "

    #Format the row data so SQL accepts it:
    row = ", ".join(repr(e) for e in rowdata)
    query = 'INSERT INTO dbo.LiveStream_Website VALUES({});'.format(row)
    #DEBUG#
    print(query)
    
    
    
    cursor.execute(query)
    conn.commit()
'''
    

        
  

#Writes the CSV data to a file.
def writeCSVData(statdata, dataLength):
    print("Sending Data to SQL Server...")
    for i in range(0, dataLength):
        ed = statdata[i]
        event_name = ''
        service_time = 0
        #Let's get some friendly values for the analytics :)
        try:
            event_name = eventnamedict[ed['eventId']]
        except:
            pass
        
        if '11am' in event_name:
            service_time = 11
        elif '9am' in event_name:
            service_time = 9
        elif '4pm' in event_name:
            service_time = 4
        elif '6pm' in event_name:
            service_time = 6
        elif '3pm' in event_name:
            service_time = 3
        elif '5pm' in event_name:
            service_time = 5
        elif '7pm' in event_name:
            service_time = 7


        clientId = ed['clientId']
        eventId = ed['eventId']
        try:
            viewTime = convertTS(ed['timestamp'])
        except:
            viewTime = ''

        ipAddress = ed['ipAddress']
        try:
            city = ed['city']
            state = ed['state']
            country = ed['country']
        except:
            city = None
            state = None
            country = None 
        
        try:
            latitude = float(ed['latitude'])
            longitude = float(ed['longitude'])
        except:
            latitude = 0
            longitude = 0

        watchTimeMinutes = ed['watchTimeMinutes']
        resolution = int(ed['resolution'])
        #userAgent = ed['userAgent']
        userAgentString = ed['userAgent']
        user_agent = parse(userAgentString)
        userAgent = user_agent.os.family
        weekday = (datetime.datetime.strptime(viewTime, '%Y-%m-%d %H:%M:%S').strftime('%A'))
        
        if weekday == 'Sunday' or 'Saturday':
            #write to a csv before doing anythinge else
            headerdata = ["clientId", "eventId", "eventName", "service_time","viewTime", "ipAddress", "city", "state", "country", "latitude", "longitude", "watchTimeMinutes", "resolution", "userAgent"]
            rowdata = [clientId,eventId,event_name, service_time, viewTime,ipAddress,city,state,country,latitude,longitude,watchTimeMinutes,resolution,userAgent]

            try:
                csv_writer.writerow(rowdata)
            except:
                sta.sendMessage("Resi Data Collection Error", "Exception was raised when attempting to write a row of data to the csv file")

            try:
                #insertSql(headerdata, rowdata)
                sql = """INSERT INTO dbo.LiveStream_Website VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) """

                cursor.execute(sql, rowdata)
                conn.commit()
            except:
                print('error with')
                print(rowdata)
                sta.sendMessage("Resi Data Collection Error", "Exception was raised when attempting to send the sql command to the database")
            
        else:
            pass


'''BEGIN EXECUTION'''

#Step 1: Login to Resi, return a new session and customer id value.
print(open_banner)
try:
    sesh, web_events = login()
except:
    sta.sendMessage("Resi Data Collection Error", "Exception was raised when attempting to get a list of web events")
print("step 2 done")
#print("webevents")
#print(web_events)


print("starting step 3")
#Get the CSV urls for each event
try: 
    events = getCSVURLS(web_events, sesh)
except:
    sta.sendMessage("Resi Data Collection Error", "Exception was raised when attempting to get a list of the csv data urls")
#print(events)
print("Step 3 done!")


#Open/create a CSV file named results.csv
try:
    data_to_file = open('results'+' '+str(timestr)+'.csv', 'w', newline='')
    csv_writer = csv.writer(data_to_file, delimiter=",")
    csv_writer.writerow(["clientId", "eventId", "eventName", "service_time","timestamp", "ipAddress", "city", "state", "country", "latitude", "longitude", "watchTimeMinutes", "resolution", "userAgent"])
except:
    sta.sendMessage("Resi Data Collection Error", "Exception was raised when attempting to create the csv document")

#Export data for each event to a file.
for expurl in events.values():
    try:
        statdata, dataLength = getCSVData(expurl)
    except:
        sta.sendMessage("Resi Data Collection Error", "Exception was raised when attempting to get the csv data")
    print("Writing Data to CSV")
    writeCSVData(statdata, dataLength)
    
data_to_file.close()
print("Data written to file")


conn.close()

print(close_banner)

print("All Done :-)")

sta.sendMessage("Resi Livestream Data Collection", "Livestream data successfully downloaded and added to the database.")

