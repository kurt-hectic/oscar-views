import requests
import pandas as pd
import math 
import re
import logging
import json
from io import StringIO

logging.getLogger(__name__).addHandler(logging.NullHandler())
logger = logging.getLogger()


country_map = json.loads('{"Algeria": "DZA", "Angola": "AGO", "Benin": "BEN", "Botswana": "BWA", "Burkina Faso": "BFA", "Burundi": "BDI", "Cabo Verde": "CPV", "Cameroon": "CMR", "Central African Republic": "CAF", "Chad": "TCD", "Comoros": "COM", "Congo": "COG", "Congo, Democratic Republic of the": "COD", "C\\u00f4te d\'Ivoire": "CIV", "Djibouti": "DJI", "Egypt": "EGY", "Equatorial Guinea": "GNQ", "Eritrea": "ERI", "Eswatini": "SWZ", "Ethiopia": "ETH", "France": "FRA", "Gabon": "GAB", "Gambia": "GMB", "Ghana": "GHA", "Guinea": "GIN", "Guinea-Bissau": "GNB", "Kenya": "KEN", "Lesotho": "LSO", "Liberia": "LBR", "Libya": "LBY", "Madagascar": "MDG", "Malawi": "MWI", "Mali": "MLI", "Mauritania": "MRT", "Mauritius": "MUS", "Morocco": "MAR", "Mozambique": "MOZ", "Namibia": "NAM", "Niger": "NGA", "Nigeria": "NGA", "Norway": "NOR", "Portugal": "PRT", "Rwanda": "RWA", "Saint Helena": "SHN", "Sao Tome and Principe": "STP", "Senegal": "SEN", "Seychelles": "SYC", "Sierra Leone": "SLE", "Somalia": "SOM", "South Africa": "ZAF", "South Sudan": "SSD", "Spain": "ESP", "Sudan": "SDN", "Tanzania, United Republic of": "TZA", "Togo": "TGO", "Tunisia": "TUN", "Uganda": "UGA", "United Kingdom (the)": "GBR", "Zambia": "ZMB", "Zimbabwe": "ZWE", "Afghanistan": "AFG", "Bahrain": "BHR", "Bangladesh": "BGD", "Cambodia": "KHM", "China": "CHN", "Hong Kong, China": "HKG", "India": "IND", "Iran, Islamic Republic of": "IRN", "Iraq": "IRQ", "Japan": "JPN", "Korea, Democratic People\'s Republic of": "PRK", "Korea, Republic of": "KOR", "Kuwait": "KWT", "Kyrgyzstan": "KGZ", "Lao People\'s Democratic Republic": "LAO", "Macao, China": "MAC", "Maldives": "MDV", "Mongolia": "MNG", "Myanmar": "MMR", "Nepal": "NPL", "Oman": "OMN", "Pakistan": "PAK", "Qatar": "QAT", "Russian Federation": "RUS", "Saudi Arabia": "SAU", "Sri Lanka": "LKA", "Tajikistan": "TJK", "Thailand": "THA", "Turkmenistan": "TKM", "United Arab Emirates (the)": "ARE", "Uzbekistan": "UZB", "Viet Nam": "VNM", "Yemen": "YEM", "Armenia": "ARM", "Austria": "AUT", "Azerbaijan": "AZE", "Belarus": "BLR", "Belgium": "BEL", "Bosnia and Herzegovina": "BIH", "Bulgaria": "BGR", "Croatia": "HRV", "Cyprus": "CYP", "Czech Republic": "CZE", "Denmark": "DNK", "Estonia": "EST", "Finland": "FIN", "Georgia": "GEO", "Germany": "DEU", "Gibraltar": "GIB", "Greece": "GRC", "Greenland": "GRL", "Hungary": "HUN", "Iceland": "ISL", "Ireland": "IRL", "Israel": "ISR", "Italy": "ITA", "Jordan": "JOR", "Latvia": "LVA", "Lebanon": "LBN", "Lithuania": "LTU", "Luxembourg": "LUX", "Malta": "MLT", "Moldova, Republic of": "MDA", "Montenegro": "MNE", "Netherlands": "NLD", "North Macedonia, Republic of": "MKD", "Poland": "POL", "Romania": "ROU", "Serbia": "SRB", "Slovakia": "SVK", "Slovenia": "SVN", "Sweden": "SWE", "Switzerland": "CHE", "Syrian Arab Republic": "SYR", "Turkey": "TUR", "Ukraine": "UKR", "Antigua and Barbuda": "ATG", "Bahamas": "BHS", "Barbados": "BRB", "Belize": "BLZ", "Canada": "CAN", "Cayman Islands": "CYM", "Colombia": "COL", "Costa Rica": "CRI", "Cuba": "CUB", "Curacao": "NLD", "Dominica": "DMA", "Dominican Republic": "DOM", "El Salvador": "SLV", "Grenada": "GRD", "Guatemala": "GTM", "Haiti": "HTI", "Honduras": "HND", "Jamaica": "JAM", "Mexico": "MEX", "Nicaragua": "NIC", "Panama": "PAN", "Puerto Rico": "PRI", "Saint Lucia": "LCA", "Saint Pierre and Miquelon": "SPM", "Sint Maarten": "NLD", "Trinidad and Tobago": "TTO", "United States (the)": "USA", "Venezuela, Bolivarian Republic of": "VEN", "Argentina": "ARG", "Bolivia, Plurinational State of": "BOL", "Brazil": "BRA", "Chile": "CHL", "Ecuador": "ECU", "Falkland Islands (Malvinas)": "FLK", "Guyana": "GUY", "Paraguay": "PRY", "Peru": "PER", "Suriname": "SUR", "Uruguay": "URY", "Australia": "AUS", "Brunei Darussalam": "BRN", "Christmas Island": "CXR", "Cocos (Keeling) Islands": "CCK", "Cook Islands": "COK", "Fiji": "FJI", "French Polynesia": "PYF", "Indonesia": "IDN", "Kiribati": "KIR", "Malaysia": "MYS", "Marshall Islands": "MHL", "Micronesia, Federated States of": "FSM", "Nauru": "NRU", "New Caledonia": "NCL", "New Zealand": "NZL", "Niue": "NIU", "Palau": "PLW", "Papua New Guinea": "PNG", "Philippines": "PHL", "Pitcairn": "PCN", "Samoa": "WSM", "Singapore": "SGP", "Solomon Islands": "SLB", "Timor-Leste": "TLS", "Tokelau": "TKL", "Tonga": "TON", "Tuvalu": "TUV", "Vanuatu": "VUT", "Kazakhstan": "KAZ"}')



def extractPrimaryWIGOSid(json):
    for station in json:
        for wigos_id in station['wigosStationIdentifiers']:
            if wigos_id['primary']:
                station['primary_WIGOS_id'] = wigos_id['wigosStationIdentifier']
                continue 
    return json

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
        
  
def mergeDFs(df_one,df_two,column,val):

        logger.debug("enter mergeDF for {}  . lena: {} lenb: {}".format(val,len(df_one),len(df_two)))

        if len(df_two) == 0:
            return df_one
            
        df_two.set_index('primary_WIGOS_id',inplace=True)
        df_two = df_two[ df_two.stationStatusCode == 'operational' ]

        remove_columns = set(df_two.columns) - set(df_one.columns)
        df_two.drop(columns=remove_columns,inplace=True)
        df_two.loc[:,column] = val # set the flag 
        
        idx_present = df_two.index.isin( df_one.index ) # the rows that are already contained in the first dataframe
        df_result = df_one.append( df_two[~idx_present] , sort=False ) # append results that are not already contained in the first
        df_result.loc[ df_result.index.isin( df_two[idx_present].index )  ,column] = val # set flag for rows that are already contained in the result        
        
        logger.debug("exit mergeDF for {}".format(val))
                
        return df_result  
        
def getMonitoring(region="africa",writeHeader=True):
      
    logger.debug("downloading data for {}".format(region))

    synop_stations = requests.get("https://oscar.wmo.int/surface/rest/api/search/station?stationClass=synopLand,synopSea&wmoRegion={}".format(region))
    radiosonde_station = requests.get("https://oscar.wmo.int/surface/rest/api/search/station?stationClass=upperAirRadiosonde&wmoRegion={}".format(region))
    radiosonde_pilot_stations = requests.get("https://oscar.wmo.int/surface/rest/api/search/station?stationClass=upperAirPilot&wmoRegion={}".format(region))
    anton_stations = requests.get("https://oscar.wmo.int/surface/rest/api/search/station?programAffiliation=ANTON,ANTONt&wmoRegion={}".format(region))

    logger.debug("constructing dataframes")
    
    mycols = ['primary_WIGOS_id', 'name', 'region', 'territory', 'latitude',  'longitude', 'Surface','Radiosonde','Radiowind','FixedShip'] 

    df_result = pd.DataFrame(  columns= mycols ).set_index('primary_WIGOS_id')
    
    df_synop =  pd.DataFrame( extractPrimaryWIGOSid( synop_stations.json() ) )
    if len(df_synop)>0:
        df_synop=df_synop[ df_synop.stationProgramsDeclaredStatuses.str.contains("RBSN",na=False)  ] 

    df_result = mergeDFs(df_result,df_synop,"Surface","S")

    df_radiosonde = pd.DataFrame( extractPrimaryWIGOSid( radiosonde_station.json() ) )
    df_result = mergeDFs(df_result,df_radiosonde,"Radiosonde","R")

    df_radiowind = pd.DataFrame( extractPrimaryWIGOSid( radiosonde_pilot_stations.json() ) )
    df_result = mergeDFs(df_result,df_radiowind,"Radiowind","W")
    
    df_anton = pd.DataFrame( extractPrimaryWIGOSid( anton_stations.json() ) )
    df_result = mergeDFs(df_result,df_anton,"Anton","A")
        
    logger.debug("data processing")

    df_result["latitude"] = df_result.latitude.apply(lambda x :  deg_min_sec(x) )
    df_result["longitude"] = df_result.longitude.apply(lambda x :  deg_min_sec(x,False) )
    df_result = df_result.fillna("")
    
    region_map = {'Africa':"I", "North America, Central America and the Caribbean": "IV" , "Europe": "VI", "South America": "III", "South-West Pacific" : "V" , "Asia" : "II" , "Antarctica" : "VII"  }
    
    df_result["RegionID"] = df_result.region.map(region_map)
    
    # only stations with 20001 or 20000 WIGOS ID 2nd block, as we can only extract WMO Indexes and Subindexes from them
    df_result=df_result[ (df_result.index.str.contains("-20000-",na=False)) | (df_result.index.str.contains("-20001-",na=False))  ]

    df_result["Index"] = df_result.reset_index().primary_WIGOS_id.str.split('-',expand=True).loc[:,3].values
    df_result["IndexSubNr"] = df_result.reset_index().primary_WIGOS_id.str.split('-',expand=True).loc[:,1].str.slice(-1).values
    
    df_result["Country/Area (VOLA)"] = df_result.territory
    df_result["Code/GLO"] = df_result.territory
    df_result["Code/GLO"]=df_result["Code/GLO"].map(country_map)
    
    col_map = { "name" : "StationName" , "region" : "RegionName" , "territory" : "Country(operator)" ,"latitude":"Latitude","longitude":"Longitude" }
    df_result.rename(columns=col_map,inplace=True)

    order = ["RegionID","RegionName","Country/Area (VOLA)","Country(operator)","Code/GLO","FixedShip","StationName","Surface","Radiosonde","Radiowind","Latitude","Longitude"]
    
    csv_buffer = StringIO()
    df_result[order].to_csv(csv_buffer, header = writeHeader ) # only print header for first chunk
    
    return csv_buffer.getvalue()