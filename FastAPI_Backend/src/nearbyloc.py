import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import geodesic



def locfinder(loc):
    geolocator = Nominatim(user_agent="MyApp")
    location = geolocator.geocode(loc)
    if(location != None):
        lat = location.latitude
        lon = location.longitude

        path = "StormEvents_details.csv"
        df_main = pd.read_csv(path, index_col=False)

        loc_info = df_main[["BEGIN_LOCATION", "BEGIN_LAT", "BEGIN_LON"]].dropna().drop_duplicates()
        loc_info["st_lat_lon"] = loc_info[["BEGIN_LAT","BEGIN_LON"]].apply(tuple, axis=1)
        loc_info['target_lat'] = lat
        loc_info['target_lon'] = lon
        loc_info['city'] = loc 
        loc_info['dis'] = 0
        loc_info = loc_info.reset_index()

        for i in range(0, len(loc_info)):
            begin_lat = loc_info.loc[i, 'BEGIN_LAT']
            begin_lon = loc_info['BEGIN_LON'][i]
            begin = (begin_lat, begin_lon)

            end_lat = loc_info['target_lat'][i]
            end_lon = loc_info['target_lon'][i]
            end = (end_lat, end_lon)

            a = geodesic(begin, end).miles
            loc_info['dis'][i] = a 
        df = loc_info[loc_info['dis'] < 100]
        df = df[df['dis']>0]
        df = df.sort_values('dis',ascending = True)
        if len(df) > 0:
            df = df.reset_index()
            begin_location = df.loc[0, :]['BEGIN_LOCATION']
            #begin_lat = df.loc[0, :]['BEGIN_LAT']
            #begin_lon = df.loc[0, :]['BEGIN_LON']
            return begin_location
        else:
            return 'NoLoc'

    else:
        return 'NoLoc'

