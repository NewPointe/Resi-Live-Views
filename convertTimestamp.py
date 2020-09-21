import datetime
import pytz

timestring = "2020-02-09T17:09:13.521Z"

def convertTS(timestring):
    # Create datetime object
    d = datetime.datetime.strptime(timestring, "%Y-%m-%dT%H:%M:%S.%fZ")
    
    source_time_zone = pytz.timezone('UTC')

    source_date_with_timezone = source_time_zone.localize(d)
    target_time_zone = pytz.timezone('US/Eastern')
    target_date_with_timezone = source_date_with_timezone.astimezone(target_time_zone)
    
    return target_date_with_timezone.strftime("%Y-%m-%d %H:%M:%S")



