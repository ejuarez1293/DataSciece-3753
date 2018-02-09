import pandas as pd
import numpy as np
from io import BytesIO
from urllib.error import URLError
import ftplib, gzip, datetime

def noaa_from_web(stationID='722530-12921', start=2016, end=2018):
    """Returns DataFrame for 1901-2017 of ISD weather data"""
    baseURL = 'ftp.ncdc.noaa.gov'
    secondaryURL = '/pub/data/noaa/'
    #stationID = '726980-24229' ## portland, OR
    df = pd.DataFrame()
    ftp = ftplib.FTP(baseURL)
    ftp.login()
    
    for year in range(start, end):
        r = BytesIO()
        url = secondaryURL + str(year) + '/' + stationID + '-' + str(year) + '.gz'
        try:
            resp = ftp.retrbinary('RETR ' + url, r.write)
            r.seek(0)
            df = df.append(pd.DataFrame(parseISD(r), columns=['Date', 'Temperature', 'Pressure', 'Humidity', 'RHPeriod', 'Sky']))
        except ftplib.error_perm:
            continue
        except URLError:
            continue
        
    ftp.close()
    df = df.set_index('Date')
    return df
    
def noaa_from_web_small(stationID='722530-12921'):   
    """Returns DataFrame for 2016-2017"""
    baseURL = 'ftp.ncdc.noaa.gov'
    secondaryURL = '/pub/data/noaa/'
    # stationID = '726980-24229' ## portland, OR
    df = pd.DataFrame()
    ftp = ftplib.FTP(baseURL)
    ftp.login()
    
    for year in range(2016, 2018):
        r = BytesIO()
        url = secondaryURL + str(year) + '/' + stationID + '-' + str(year) + '.gz'
        try:
            resp = ftp.retrbinary('RETR ' + url, r.write)
            r.seek(0)
            df = df.append(pd.DataFrame(parseISD(r), columns=['Date', 'Temperature', 'Pressure', 'Humidity', 'RHPeriod', 'Sky']))
        except ftplib.error_perm:
            continue
        except URLError:
            continue
        
    ftp.close()
    df = df.set_index('Date')
    return df
   
def parseISD(data):
    """Parse date, time, temp, pressure, skycond, humidity from ISD data as dict
    
    data = gzip file
    relavent excepts from the ISD documentation: 
    
    POS: 16-23
        GEOPHYSICAL-POINT-OBSERVATION date
        The date of a GEOPHYSICAL-POINT-OBSERVATION.
        MIN: 00000101 MAX: 99991231
        DOM: A general domain comprised of integer values 0-9 in the format YYYYMMDD.
            YYYY can be any positive integer value; MM is restricted to values 01-12; and DD is restricted
            to values 01-31.
            
    POS: 24-27
        GEOPHYSICAL-POINT-OBSERVATION time
        The time of a GEOPHYSICAL-POINT-OBSERVATION based on
        Coordinated Universal Time Code (UTC).
        MIN: 0000 MAX: 2359
        DOM: A general domain comprised of integer values 0-9 in the format HHMM.
            HH is restricted to values 00-23; MM is restricted to values 00-59.
    
    
    
    POS: 88-92
        AIR-TEMPERATURE-OBSERVATION air temperature
        The temperature of the air.
        MIN: -0932 MAX: +0618 UNITS: Degrees Celsius
        SCALING FACTOR: 10
        DOM: A general domain comprised of the numeric characters (0-9), a plus sign (+), and a minus
            sign (-).
            +9999 = Missing.
            
    
    POS: 100-104
        ATMOSPHERIC-PRESSURE-OBSERVATION sea level pressure 
        The air pressure relative to Mean Sea Level (MSL).
        MIN: 08600 MAX: 10900 UNITS: Hectopascals
        SCALING FACTOR: 10
        DOM: A general domain comprised of the numeric characters (0-9).
            99999 = Missing.
            
    FLD LEN: 3
        SKY-CONDITION-OBSERVATION identifier
        An indicator that denotes the start of a SKY-CONDITION-OBSERVATION data group.
        DOM: A specific domain comprised of the characters in the ASCII character set.
        GF1: An indicator of the occurrence of the following data items:
            SKY-CONDITION-OBSERVATION total coverage code
            SKY-CONDITION-OBSERVATION total opaque coverage code
            SKY-CONDITION-OBSERVATION quality total coverage code
            SKY-CONDITION-OBSERVATION total lowest cloud cover code
            SKY-CONDITION-OBSERVATION quality total lowest cloud cover code
            SKY-CONDITION-OBSERVATION low cloud genus code
            SKY-CONDITION-OBSERVATION quality low cloud genus code
            SKY-CONDITION-OBSERVATION lowest cloud base height dimension
            SKY-CONDITION-OBSERVATION lowest cloud base height quality code
            SKY-CONDITION-OBSERVATION mid cloud genus code
            SKY-CONDITION-OBSERVATION quality mid cloud genus code
            SKY-CONDITION-OBSERVATION high cloud genus code
            SKY-CONDITION-OBSERVATION quality high cloud genus code
        
    FLD LEN: 2
        SKY-CONDITION-OBSERVATION total coverage code
        The code that denotes the fraction of the total celestial dome covered by clouds or other obscuring
        phenomena.
        DOM: A specific domain comprised of the characters in the ASCII character set.
            00: None, SKC or CLR
            01: One okta - 1/10 or less but not zero
            02: Two oktas - 2/10 - 3/10, or FEW
            03: Three oktas - 4/10
            04: Four oktas - 5/10, or SCT
            05: Five oktas - 6/10
            06: Six oktas - 7/10 - 8/10
            07: Seven oktas - 9/10 or more but not 10/10, or BKN
            08: Eight oktas - 10/10, or OVC
            09: Sky obscured, or cloud amount cannot be estimated
            10: Partial obscuration
            11: Thin scattered
            12: Scattered
            13: Dark scattered
            14: Thin broken
            15: Broken
            16: Dark broken
            17: Thin overcast
            18: Overcast
            19: Dark overcast
            99: Missing

    FLD LEN: 3
        RELATIVE HUMIDITY occurrence identifier
        The identifier that denotes the start of a RELATIVE-HUMIDITY data section
        DOM: A specific domain comprised of the characters in the ASCII character set
            RH1 – RH3 An indicator of up to 3 occurrences of the following items
                RELATIVE HUMIDITY period quantity
                RELATIVE HUMIDITY code
                RELATIVE HUMIDITY percentage
                RELATIVE HUMIDITY derived code
                RELATIVE HUMIDITY quality code
                
    FLD LEN: 3
        RELATIVE HUMIDITY period quantity
        The quantity of time over which relative humidity percentages were averaged to determine the RELATIVE HUMIDITY
        MIN: 001 MAX: 744 UNITS: Hours
        SCALING FACTOR: 1
        DOM: A general domain comprised of the numeric characters (0-9)
        999 = missing
        FLD LEN: 1
        RELATIVE HUMIDITY code
        The code that denotes the RELATIVE HUMIDITY as an average, maximum or minimum
        DOM: A specific domain comprised of the characters in the ASCII character set
            M: Mean relative humidity
            N: Minimum relative humidity
            X: Maximum relative humidity
            9 = missing
            
    FLD LEN: 3
    RELATIVE HUMIDITY percentage
        The average maximum or minimum relative humidity for a given period, typically for the day or month, derived from other
        data fields. Note: Values only take into account hourly observations (not specials or other unscheduled observations).
        MIN: 000 MAX: 100 UNITS: percent
        SCALING FACTOR: 1
        DOM: A general domain comprised of the numeric characters (0-9).
        999 = missing
        FLD LEN: 1
        RELATIVE HUMIDITY derived code
        The code that denotes a derived code of the reported RELATIVE HUMIDITY percentage.
        DOM: A specific domain comprised of the characters in the ASCII character set.
            D = Derived from hourly values
            9 = missing
        
    FLD LEN: 1
        RELATIVE HUMIDITY quality code
        The code that denotes a quality status of the reported RELATIVE HUMIDITY percentage
        DOM: A specific domain comprised of the characters in the ASCII character set.
            0 = Passed gross limits check
            1 = Passed all quality control checks
            2 = Suspect
            3 = Erroneous
            4 = Passed gross limits check, from NCEI ASOS/AWOS
            5 = Passed all quality control checks, from NCEI ASOS/AWOS
            6 = Suspect, from NCEI ASOS/AWOS
            7 = Erroneous, from NCEI ASOS/AWOS
            9 = Missing

    """
    
    dates = list()
    temps = list()
    pressures = list()
    skys = list()
    rhs = list()
    rhhrs = list()
    
    for line in gzip.open(data).readlines():
        line = str(line)
        dt = datetime.datetime.strptime(line[17:29], '%Y%m%d%H%M')
        temp = int(line[89:94])
        if temp == 9999:
            temp = np.NaN
        pressure = int(line[101:106])
        if pressure == 99999:
            pressure = np.NaN
        skycond = np.NaN
        try:
            index = line.index('GF1')
            tmpcond = int(line[index + 3: index + 5])
            if tmpcond != 99:
                skycond = tmpcond
        except ValueError:
            pass
        
        mean_rh = np.NaN
        mean_rh_hrs = np.NaN
        # only getting the mean humidity
        for field in ['RH1', 'RH2', 'RH3']:
            try:
                index = line.index(field)
                rh = line[index + 3: index + 10]
                if rh[3] == 'M':
                    mean_rh_hrs = int(rh[:2])
                    mean_rh = int(rh[4:])
            except ValueError:
                pass
        
                
        dates.append(dt)
        temps.append(temp)
        pressures.append(pressure)
        rhs.append(mean_rh)
        rhhrs.append(mean_rh_hrs) 
        skys.append(skycond)
        
    return {'Date' : dates, 'Temperature' : temps, 'Pressure' : pressures, 'Humidity' : rhs, 'RHPeriod' : rhhrs, 'Sky':skys}
        
     