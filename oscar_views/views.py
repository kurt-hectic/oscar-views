import requests
import pandas as pd
import math 
import pycountry
import re
import logging
from io import StringIO

logging.getLogger(__name__).addHandler(logging.NullHandler())
logger = logging.getLogger()


def extractPrimaryWIGOSid(json):
    for station in json:
        for wigos_id in station['wigosStationIdentifiers']:
            if wigos_id['primary']:
                station['primary_WIGOS_id'] = wigos_id['wigosStationIdentifier']
                continue 
    return json

def resolveCountryName(countryname):
    
    m = re.search("(.*),(.*)",countryname)
    if m:
        countryname = "{} ({})".format(m.group(1).strip(),m.group(2).strip())
        
    countryname = countryname.replace('(the)','').strip()
        
    r2 = requests.get("https://restcountries.eu/rest/v2/name/{}?fields=alpha3Code".format(countryname))
    if r2.status_code != requests.codes.ok:
        problem=True
        if m:
            countryname=m.group(1).strip()
            r2 = requests.get("https://restcountries.eu/rest/v2/name/{}?fields=alpha3Code".format(countryname))
            if r2.status_code == requests.codes.ok:
                problem=False

        if problem:
            print("problem mapping '{}'".format(countryname))
            return False

    return r2.json()[0]["alpha3Code"]

def deg_min_sec(degrees = 0.0, latitude=True):
    if type(degrees) != 'float':
        try:
            degrees = float(degrees)
        except:
            print('\nERROR: Could not convert %s to float.' %(type(degrees)))
            return 0
    minutes = degrees%1.0*60
    seconds = minutes%1.0*60
    
    degrees = int(math.floor(degrees))
    minutes = int(math.floor(minutes))
    
    if latitude:
        prefix = "N" if degrees>0 else "S" 
    else:
        prefix = "E" if degrees>0 else  "W"

    if latitude:
        return "{:02d} {:02d}{}".format( abs(degrees), minutes, prefix)
    else:
        return "{:03d} {:02d}{}".format( abs(degrees), minutes, prefix)
        
        
        
def getMonitoring(region="africa"):
      
    logger.debug("downloading data")

    synop_stations = requests.get("https://oscar.wmo.int/surface/rest/api/search/station?stationClass=synopLand,synopSea&wmoRegion={}".format(region))
    radiosonde_station = requests.get("https://oscar.wmo.int/surface/rest/api/search/station?stationClass=upperAirRadiosonde&wmoRegion={}".format(region))
    radiosonde_pilot_stations = requests.get("https://oscar.wmo.int/surface/rest/api/search/station?stationClass=upperAirPilot&wmoRegion={}".format(region))

    logger.debug("constructing dataframes")
    
    #mycolumns = ['wigosStationIdentifiers','id','declaredStatus','elevation','stationTypeId','dateEstablished','dateClosed'] 
    mycolumns = ['wigosStationIdentifiers','id','declaredStatus','elevation','stationTypeId','dateEstablished'] 
    
    df_synop =  pd.DataFrame( extractPrimaryWIGOSid( synop_stations.json() ) )

    df_synop["Surface"] = "S"
    df_synop.drop(columns=mycolumns,inplace=True)

    df_synop=df_synop[ (df_synop.stationProgramsDeclaredStatuses.str.contains("RBSN",na=False)) & ( df_synop.stationStatusCode == 'operational' ) ] 
    df_synop.set_index('primary_WIGOS_id',inplace=True)
    
    df_radiosonde = pd.DataFrame( extractPrimaryWIGOSid( radiosonde_station.json() ) )
    has_rs_results = len(df_radiosonde) > 0
    
    if has_rs_results:
        df_radiosonde["Radiosonde"] = "R"
        df_radiosonde.set_index('primary_WIGOS_id',inplace=True)
        df_radiosonde.drop(columns=mycolumns,inplace=True)
        df_radiosonde = df_radiosonde[ df_radiosonde.stationStatusCode == 'operational' ]


    df_radiowind = pd.DataFrame( extractPrimaryWIGOSid( radiosonde_pilot_stations.json() ) )
    has_rw_results = len(df_radiowind) > 0
    
    if has_rw_results :
        df_radiowind["Radiowind"] = "W"
        df_radiowind.set_index('primary_WIGOS_id',inplace=True)
        df_radiowind.drop(columns= set(df_radiowind.columns).intersection(set(mycolumns))   ,inplace=True)
        df_radiowind = df_radiowind[ df_radiowind.stationStatusCode == 'operational' ]

    logger.debug("joinging dataframes")
        
    # need to append only those rows not already contained.. the others are joined to existing

    if has_rs_results:
        idx_radiosonde = ~df_radiosonde.index.isin( df_synop.index )
        df_tmp = df_synop.append( df_radiosonde[idx_radiosonde] ).join( df_radiosonde[~idx_radiosonde]["Radiosonde"] , rsuffix='_temp', sort=False )

        df_tmp.loc[ ~df_tmp["Radiosonde_temp"].isna() , ["Radiosonde"]] =  df_tmp[ ~df_tmp["Radiosonde_temp"].isna()  ]["Radiosonde_temp"]
        df_tmp.drop(columns=['Radiosonde_temp'],inplace=True)
    else:
        df_tmp["Radiosonde"] = ""

    # need to append only those rows not already contained.. the others are joined to existing

    if has_rw_results:
        idx_radiowind = ~df_radiowind.index.isin( df_tmp.index )
        df_tmp = df_tmp.append( df_radiowind[idx_radiowind] ).join( df_radiowind[~idx_radiowind]["Radiowind"] , rsuffix='_temp', sort=False )

        df_tmp.loc[ ~df_tmp["Radiowind_temp"].isna() , ["Radiowind"]] =  df_tmp[ ~df_tmp["Radiowind_temp"].isna()  ]["Radiowind_temp"]
        df_tmp.drop(columns=['Radiowind_temp'],inplace=True)
    else:
        df_tmp["Radiowind"] = ""

    logger.debug("data processing")

    df_tmp["latitude"] = df_tmp.latitude.apply(lambda x :  deg_min_sec(x) )
    df_tmp["longitude"] = df_tmp.longitude.apply(lambda x :  deg_min_sec(x,False) )
    df_tmp = df_tmp.fillna("")
    
    region_map = {'Africa':"I", "North America, Central America and the Caribbean": "IV" , "Europe": "VI", "South America": "III", "South-West Pacific" : "V" , "Asia" : "II" , "Antarctica" : "VII"  }
    
    df_tmp["RegionID"] = df_tmp.region.map(region_map)
    
    # only stations with 20001 or 20000 WIGOS ID 2nd block, as we can only extract WMO Indexes and Subindexes from them
    df_tmp=df_tmp[ (df_tmp.index.str.contains("-20000-",na=False)) | (df_tmp.index.str.contains("-20001-",na=False))  ]

    df_tmp["Index"] = df_tmp.reset_index().primary_WIGOS_id.str.split('-',expand=True).loc[:,3].values
    df_tmp["IndexSubNr"] = df_tmp.reset_index().primary_WIGOS_id.str.split('-',expand=True).loc[:,1].str.slice(-1).values
    
    df_tmp["Country/Area (VOLA)"] = df_tmp.territory
    df_tmp["Code/GLO"] = df_tmp.territory
    df_tmp["FixedShip"] = ""

    col_map = { "name" : "StationName" , "region" : "RegionName" , "territory" : "Country(operator)" ,"latitude":"Latitude","longitude":"Longitude" }

    df_tmp.rename(columns=col_map,inplace=True)


    order = ["RegionID","RegionName","Country/Area (VOLA)","Country(operator)","Code/GLO","FixedShip","StationName","Surface","Radiosonde","Radiowind","Latitude","Longitude"]


    country_map = {}
    for c in df_tmp["Country(operator)"].unique():
        try:
            t=pycountry.countries.search_fuzzy(c)
            t=t[0]
            country_map[c]=t.alpha_3
            #print("{} to {}".format(c,t))
        except Exception:
            pass
            
    country_map["North Macedonia, Republic of"] = "MKD"
    country_map["United States (the)"] = "USA"
    country_map["Congo, Democratic Republic of the"] = "COD"
    country_map["United Kingdom (the)"] = "GBR"
    country_map["Macao, China"] = "MAC"
    country_map["United Arab Emirates (the)"] = "ARE"

    df_tmp["Code/GLO"]=df_tmp["Code/GLO"].map(country_map)
    
    csv_buffer = StringIO()
    
    df_tmp[order].to_csv(csv_buffer)
    
    
    return csv_buffer