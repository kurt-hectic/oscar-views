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

    logger.debug("joining dataframes")
        
    # need to append only those rows not already contained.. the others are joined to existing

    if has_rs_results:
        idx_radiosonde = ~df_radiosonde.index.isin( df_synop.index )
        df_tmp = df_synop.append( df_radiosonde[idx_radiosonde] , sort=False ).join( df_radiosonde[~idx_radiosonde]["Radiosonde"] , rsuffix='_temp', sort=False )

        df_tmp.loc[ ~df_tmp["Radiosonde_temp"].isna() , ["Radiosonde"]] =  df_tmp[ ~df_tmp["Radiosonde_temp"].isna()  ]["Radiosonde_temp"]
        df_tmp.drop(columns=['Radiosonde_temp'],inplace=True)
    else:
        df_tmp["Radiosonde"] = ""

    # need to append only those rows not already contained.. the others are joined to existing

    if has_rw_results:
        idx_radiowind = ~df_radiowind.index.isin( df_tmp.index )
        df_tmp = df_tmp.append( df_radiowind[idx_radiowind] , sort=False ).join( df_radiowind[~idx_radiowind]["Radiowind"] , rsuffix='_temp', sort=False )

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

    df_tmp["Code/GLO"]=df_tmp["Code/GLO"].map(country_map)
    
    csv_buffer = StringIO()
    
    df_tmp[order].to_csv(csv_buffer ,  index = region == "africa" ) # only print header for first chunk
    
    
    return csv_buffer.getvalue()