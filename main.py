import pandas as pd
import numpy as np
import sys
import datetime
import re
import tqdm
from concurrent.futures import ProcessPoolExecutor
import xarray as xr
import os
from pathlib import Path
import math
import requests
import json

import logging

logging.basicConfig(filename='main.log',level=logging.INFO)

API_KEY="<INSERT_YOUR_API_KEY_HERE>"

def convert_dta_to_csv(dta_path):
    df = pd.read_stata(dta_path)
    df.to_csv(f"{dta_path.split('.')[0]}.csv", index=False)
    return 



def show_nrow_csv(csv_path,nrows):

    df = pd.read_csv(csv_path,nrows=nrows)
    print(df[['start_date','end_date']])


def create_date_range(df):
    all_dates = set()  # Use a set to avoid duplicate dates
    for index, row in df.iterrows():
        start_date = pd.to_datetime(row['start_date'])
        end_date = pd.to_datetime(row['end_date'])
        
        # Generate dates from start_date to end_date
        while start_date <= end_date:
            all_dates.add(start_date.strftime('%Y-%m-%d'))
            start_date += datetime.timedelta(days=1)
        
        # Generate dates for one to five days before the original start_date
        for i in range(1, 6):
            before_start_date = (pd.to_datetime(row['start_date']) - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            all_dates.add(before_start_date)

    # Convert set to list and sort
    sorted_dates = sorted(list(all_dates))

    # Write dates to a text file
    with open('dates.txt', 'w') as f:
        for date in sorted_dates:
            f.write("%s\n" % date)



def process_chunk(chunk):
    logging.info('start geting chunk')
    all_dates = []
    for index, row in chunk.iterrows():
        start_date = pd.to_datetime(row['startDate'])
        end_date = pd.to_datetime(row['endDate'])
        
        # Dates for one to five days before the start_date
        for i in range(1, 6):
            before_start_date = start_date - datetime.timedelta(days=i)
            all_dates.append(before_start_date.strftime('%Y-%m-%d'))

        # Dates from start_date to end_date
        while start_date <= end_date:
            all_dates.append(start_date.strftime('%Y-%m-%d'))
            start_date += datetime.timedelta(days=1)
    
    return all_dates
    




def extract_dates(path_text):

    with open('dates.txt','r') as file:
        competition_dates_str = file.read()

    competition_dates_list = competition_dates_str.split('\n')

    with open(path_text) as file:
        data = file.read()


    date_pattern = re.compile(r'.*\.(\d{8})\.nc4')

    lines = data.split('\n')
    extracted_urls = []
    for line in lines:
        match = date_pattern.match(line)
        if match:
            # Convert the matched date string to a datetime object if necessary
            extracted_date = match.group(1)
            extracted_date_obj = datetime.datetime.strptime(extracted_date,'%Y%m%d')
            if extracted_date_obj.strftime('%Y-%m-%d') in competition_dates_list:
                extracted_urls.append(line)
    with open(path_text.split('.')[0]+'download_url.txt','w') as file:
        file.write('\n'.join(extracted_urls))



def parallel_process_chunks(chunks):
    try:
        with ProcessPoolExecutor() as executor:
            results = list(executor.map(process_chunk, chunks))
        return results
    except Exception as e:
        logging.error(f"Error processing chunks: {e}",exc_info=True)
        return []

def create_cookies(cookie_text):
    """
        This function get cookies in text format and then extracts it's keys and values in dictionary format
    """
    cookies: dict = {}
    for cookie_variable in cookie_text.split(';'):
        cookies[cookie_variable.split(
            '=')[0].strip()] = cookie_variable.split("=")[1].strip()
    return cookies

def create_pandas_dataframe_and_delete_file_O3(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()
    # get only the useful columns
    # ['PS', 'DELP', 'T', 'U', 'V', 'QV', 'O3']

    # remove time from index in order to use groupby
    df.reset_index(['time'], inplace=True)

    df = df.groupby(['lon', 'lat','lev']).mean()
    df.drop("time", inplace=True, axis=1)
    df.reset_index(['lev'], inplace=True)

    df = df.groupby(['lon', 'lat']).mean()
    df.drop("lev", inplace=True, axis=1)

    df.to_csv(Path(f'O3/{date}.csv'))
    os.remove(name)
def create_pandas_dataframe_and_delete_file_T2(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()
    # get only the useful columns

    df=df[['T2MMEAN','TPRECMAX']]

    df.to_csv(Path(f'T2/{date}.csv'))
    os.remove(name)
def create_pandas_dataframe_and_delete_file_Rainfall(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()
    df.reset_index(level=['time','bnds'],inplace=True)
    df = df[df['bnds']==1]
    # get only the useful columns
    df=df[['Rainf_f_tavg', 'Tair_f_tavg', 'Qtotal_tavg']]
    
    df.to_csv(Path(f'Rainf/{date}.csv'))
    os.remove(name)

def create_pandas_dataframe_and_delete_file_Rainfall2(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()
    df.reset_index(level=['time','bnds'],inplace=True)
    df = df[df['bnds']==1]
    # get only the useful columns
    df=df[['Rainf_f_tavg', 'Rainf_tavg']]
    
    df.to_csv(Path(f'Rainf2/{date}.csv'))
    os.remove(name)

def create_pandas_dataframe_and_delete_file_PM25(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()

    df = df[['DUSMASS25','OCSMASS','BCSMASS','SSSMASS25','SO4SMASS']]
    df.reset_index(level=['time'],inplace=True)
    df = df.groupby(['lon','lat']).mean()
    df.drop('time',inplace=True,axis=1)
    df['PM25'] = df['DUSMASS25'] + df['OCSMASS'] + df['BCSMASS'] + df['SSSMASS25'] + df['SO4SMASS']*(132.14/96.06)
    df = df[['PM25']]
    print(f'start writing PM25/{date}.csv')
    df.to_csv(Path(f'PM25_cacl/{date}.csv'))
    print(f'finish writing PM25/{date}.csv')
    os.remove(name)
def create_pandas_dataframe_and_delete_file_PM25_second_way(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()

    df.reset_index(level=['time'],inplace=True)
    df = df.groupby(['lon','lat']).mean()
    df.drop('time',inplace=True,axis=1)
    df = df[['MERRA2_CNN_Surface_PM25']]
    # print(f'start writing PM25/{date}.csv')
    df.to_csv(Path(f'PM25/{date}.csv'))
    # print(f'finish writing PM25/{date}.csv')
    os.remove(name)

def create_pandas_dataframe_and_delete_file_TO3(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds.to_dataframe()

    df.reset_index(level=['time'],inplace=True)
    df['time'] = pd.to_datetime(df['time'])
    df1 = df.loc[(df['time'].dt.hour >= 6) & (df['time'].dt.hour <= 18)]
    df1 = df1[['T2M']]
    df = df[['U2M','V2M','TO3']]
    df = df.groupby(['lon','lat']).mean()
    df1 = df1.groupby(['lon','lat']).mean()
    df['T2M'] = df1['T2M']
    # print(f'start writing PM25/{date}.csv')
    df.to_csv(Path(f'TO3/{date}.csv'))
    # print(f'finish writing PM25/{date}.csv')
    os.remove(name)

def create_pandas_dataframe_and_delete_file_RH(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds[['RH']].to_dataframe()

    df = df.reset_index(level=['lev','time'],drop=True).groupby(['lat','lon']).mean()
    df.to_csv(Path(f'RH/{date}.csv'))
    os.remove(name)

def create_pandas_dataframe_and_delete_file_COSC(name, date):
    # Open the ncf4 file
    ds = xr.open_dataset(name)

    # Convert it to dataframe
    df = ds[['COSC','TO3']].to_dataframe()

    df.reset_index(level=['time'],drop=True, inplace=True)

    df = df.groupby(['lon','lat']).mean()
    # print(f'start writing PM25/{date}.csv')
    df.to_csv(Path(f'COSC/{date}.csv'))
    # print(f'finish writing PM25/{date}.csv')
    os.remove(name)
    

def create_json_from_text_file(filename='cities.text'):
    """Creates a JSON object from a cities.text file containing city coordinates.

    Returns:
        A JSON object containing the city coordinates.
    """
    with open(filename, "r") as file:
        lines = file.readlines()
    coordinates = {}
    for line in lines:
        line = " ".join(line.strip().split())
        city_data = line.split(" ")
        try:
            float(city_data[-2])
            float(city_data[-1])
            coordinates[" ".join(city_data[:-2])] = {
                "lat": city_data[-2],
                "lng": city_data[-1],
            }
        except:
            coordinates[" ".join(city_data[:])] = {"lat": None, "lng": None}

    return coordinates


def getDistanceFromLatLonInKm(lat1,lon1,lat2,lon2):
    """
        this function use  Haversine formula  to calculate the distance between two coordinates.

     Args:
        lat1: The latitude of the first coordinate.
        lon1: The longitude of the first coordinate.
        lat2: The latitude of the second coordinate.
        lon2: The longitude of the second coordinate.

    Returns:
        The distance between the two coordinates in kilometers.

    Refrences: 
     - https://www.movable-type.co.uk/scripts/latlong.html
     - https://en.wikipedia.org/wiki/Haversine_formula
    """

    radius = 6371 # radius of earth in kilometer
    dlat = math.radians(lat2 - lat1) # convert degree to radian
    dlon = math.radians(lon2 - lon1) # convert degree to radian


    # calculate a in https://www.movable-type.co.uk/scripts/latlong.html:  a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    # calculate c in https://www.movable-type.co.uk/scripts/latlong.html: c = 2 ⋅ atan2( √a, √(1−a) )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = radius * c

    return distance




def get4NearestPoints(lat,lon,lat_range=[], lon_range=[]):
    """ Returns the four nearest latitude and longitude coordinates to the given latitude and longitude.

    Args:
        lat: The latitude coordinate.
        lon: The longitude coordinate.

    Returns:
        A tuple of two lists, containing the four nearest latitude and longitude coordinates.
    """
    if not (len(lat_range) and len(lon_range)):
        lon_range = np.arange(-180,180,0.625)
        lat_range = np.arange(-90,90,0.5)


    closest_smaller_lat = lat_range[lat_range < lat].max() if np.any(lat_range<lat) else None
    close_bigger_lat = lat_range[lat_range > lat].min() if np.any(lat_range>lat) else None
    lats = [closest_smaller_lat.round(2),close_bigger_lat.round(2)]

    closest_smaller_lon = lon_range[lon_range < lon].max() if np.any(lon_range < lon) else None
    closest_bigger_lon = lon_range[lon_range > lon].min() if np.any(lon_range > lon) else None
    lons = [closest_smaller_lon.round(3),closest_bigger_lon.round(3)]
    return lats,lons


def findNearestCoordinatesInKm(lat,lng):
    """ Finds the nearest latitude and longitude coordinates to the given latitude and longitude, in kilometers.

        Args:
            lat: The latitude coordinate.
            lng: The longitude coordinate.

        Returns:
            A dictionary containing the nearest latitude and longitude coordinates, and the distance to those coordinates in kilometers.
    """
    lats,lons = get4NearestPoints(lat,lng)
    nearestLatLon = {'lat':None,'lon':None,'distance':10000000}
    for la in lats:
        for lo in lons:
            distance = getDistanceFromLatLonInKm(lat,lng,la,lo)
            if distance < nearestLatLon['distance']:
                nearestLatLon['distance'] = distance
                nearestLatLon['lat'] = la
                nearestLatLon['lon'] = lo
    return nearestLatLon


def extract_city_name(name):
    split_name = name.split(',')
    if len(split_name)==2:
        name = split_name[1]
    elif len(split_name) == 3:
      return split_name[1].strip().lower()
    name = name[:name.find(' (')].strip()
    if name in ['PA','NC','AR','KS','WA','DC','CA','WI','MI','MD','VA','SC','AL','OR','MN','FL','NM','TX','TN','MA','IA','NJ','KY','NY','AZ','GA','CO','ME','HI','IL','IN','CT','LA','OH','NV','WV']:
        name = split_name[0].strip()
    return name.lower()

def remove_already_saved_cities(places,file='cities.text'):
    with open(file, "r") as file:
        saved_cities_with_coordinates = file.read()

    saved_cities = []
    for city in saved_cities_with_coordinates.split("\n"):
        city = " ".join(city.strip().split())
        city_data = city.split(" ")
        try:
            float(city_data[-2])
            float(city_data[-1])
            saved_cities.append(" ".join(city_data[:-2]))

        except:
            print(
                f"lat and lng of this city is not specified: {' '.join(city_data[:])}"
            )

    new_cities = []
    for city in places:
        if city not in saved_cities:
            new_cities.append(city)

    return new_cities


def extract_lat_lng(df):
    all_cities = list(set(df['city']))
    all_cities = remove_already_saved_cities(places=all_cities,file='cities.text')

    print("number of remaining cities: ", len(all_cities))
    coordinates = {}
    # Print the coordinates in a table format
    print("{:<20} {:<15} {:<15}".format("City", "Latitude", "Longitude"))
    print("-" * 50)
    for city in all_cities:
        url = f"https://api.opencagedata.com/geocode/v1/json?q={city}&key={API_KEY}"
        response = requests.get(url)
        data = response.json()
        # Extract latitude and longitude if available
        if "results" in data and len(data["results"]) > 0:
            lat = data["results"][0]["geometry"]["lat"]
            lng = data["results"][0]["geometry"]["lng"]
            coordinates[city] = {"Latitude": lat, "Longitude": lng}
        else:
            coordinates[city] = {"Latitude": None, "Longitude": None}
            lat = ""
            lng = ""
        print("{:<20} {:<15} {:<15}".format(city, lat, lng))
        with open("cities.text", "a") as file:
            file.write("{:<20} {:<15} {:<15}\n".format(city, lat, lng))
    with open("all_cities.json", "w") as file:
        json.dump(coordinates, file)

def extract_lat_lng_from_venue():
    df = pd.read_csv('data_with_city_coordinates_remove_nan_lat_lng.csv')
    address = df.groupby('venue').first()
    all_address = list(set(df['venue']))    
    all_address = remove_already_saved_cities(places=all_address,file='venue.text')


    print("number of remaining venue: ", len(all_address))
    coordinates = {}
    # Print the coordinates in a table format
    print("{:<50} {:<15} {:<15}".format("Venue", "Latitude", "Longitude"))
    print("-" * 50)
    for addr in all_address:
        url = f"https://api.opencagedata.com/geocode/v1/json?q={addr}&key={API_KEY}"
        response = requests.get(url)
        data = response.json()
        # Extract latitude and longitude if available
        if "results" in data and len(data["results"]) > 0:
            lat = data["results"][0]["geometry"]["lat"]
            lng = data["results"][0]["geometry"]["lng"]
            coordinates[addr] = {"Latitude": lat, "Longitude": lng}
        else:
            coordinates[addr] = {"Latitude": None, "Longitude": None}
            lat = ""
            lng = ""
        print("{:<50} {:<15} {:<15}".format(addr, lat, lng))
        with open("venue.text", "a") as file:
            file.write("{:<50} {:<15} {:<15}\n".format(addr, lat, lng))
    with open("new_venue.json", "w") as file:
        json.dump(coordinates, file)





def refined_cities(df):
    df.loc[df['venue']=='Stadion TJ Lokomotiva Olomouc,, Olomouc (CZE)','city'] = 'olomouc'
    df.loc[df['venue']=='Stadion TJ Lokomotiva Olomouc,, Olomouc (CZE) (i)','city'] = 'olomouc'
    df.loc[df['venue']=='Pista de Atletismo Río Esgueva,, Valladolid (ESP)','city'] = 'valladolid'
    df.loc[df['venue']=='Pista Atmo. Campo de la Juventud,, Palencia (ESP)','city'] = 'palencia'
    df.loc[df['venue']=='Kropyvnytskiy (UKR)','city'] = 'kropyvnytskyi' # spell of the city name is wrong in the website
    df.loc[df['venue']=='Kopvogur (ISL)','city'] = 'kopavogur' # spell of the city name is wrong in the website
    df.loc[df['venue']=='Belaya Tserkva (UKR)','city'] = 'bila tserkva' # spell of the city name is wrong in the website
    df.loc[df['venue']=='Alexandropoulis (GRE)','city'] = 'alexandroupoli' # spell of the city name is wrong in the website
    df.loc[df['venue']=='Shahrekod (IRI)','city'] = 'shahrekord' # spell of the city name is wrong in the website
    df.loc[df['venue']=='Naypiydaw (MYA)','city'] = 'naypyidaw' # spell of the city name is wrong in the website
    df.loc[df['venue']=='Stade du Buisson de la Grolle, La Chapele-sur-Erdre (FRA)','city'] = 'la chapelle-sur-erdre' # spell of the city name is wrong in the website
    df.loc[df['venue']=='5th Ave. NYC Streets,Grand Army Plaza, New York, NY (USA)','city'] = 'new york' # spell of the city name is wrong in the website



if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == 'c2c':
            dta_path = 'raw_data.dta'
            convert_dta_to_csv(dta_path)
        elif sys.argv[1] == 'show-rows':
            csv_path = 'raw_data.csv'
            show_nrow_csv(csv_path,10)
        elif sys.argv[1] == 'get-dates':
            csv_path = 'data_with_city_name.csv'
            df = pd.read_csv(csv_path,usecols=['startDate','endDate'])
            chunks = np.array_split(df, 12)
            chunk_results = parallel_process_chunks(chunks)
            # Flatten the list of lists
            all_dates = [date for sublist in chunk_results for date in sublist]

            # Remove duplicates and sort
            unique_sorted_dates = sorted(set(all_dates))

            # Save the dates to a text file
            with open('dates.txt', 'w') as f:
                for date in unique_sorted_dates:
                    f.write(f"{date}\n")
        elif sys.argv[1] == 'gen-download-links':
            extract_dates('NASA/subset_M2SDNXSLV_5.12.4_20240222_140743_.txt')
        elif sys.argv[1] == 'o3':
            name = 'MERRA2_300.inst6_3d_ana_Nv.20050109.nc4'
            create_pandas_dataframe_and_delete_file_O3(name,date)

        elif sys.argv[1] == 'cities':
            df = pd.read_csv("data_with_city_name.csv")
            extract_lat_lng(df)

            coordinates = create_json_from_text_file()
            coordinates_df = pd.DataFrame.from_dict(coordinates, orient="index")
            coordinates_df.reset_index(inplace=True)
            coordinates_df.columns = ["city", "lat", "lng"]
            coordinates_df.to_csv('cities.csv')
            df = pd.merge(df, coordinates_df, on="city", how="left")
            # df.drop("details.1", inplace=True, axis=1)
            print("after: ", df.shape)
            # df.to_csv("update_with_coordinates.csv")
            df.to_csv('data_with_city_coordinates.csv')


        elif sys.argv[1] == 'address':
            print('start extracting lat and lng from address')
            # extract_lat_lng_from_venue()
            df = pd.read_csv("data_with_city_coordinates_remove_nan_lat_lng.csv")
            coordinates = create_json_from_text_file(filename='venue.text')
            coordinates_df = pd.DataFrame.from_dict(coordinates, orient="index")
            coordinates_df.reset_index(inplace=True)
            coordinates_df.columns = ["venue", "lat_addr", "lng_addr"]
            coordinates_df.to_csv('venue.csv')
            df = pd.merge(df, coordinates_df, on="venue", how="left")
            # df.drop("details.1", inplace=True, axis=1)
            print("after: ", df.shape)
            # df.to_csv("update_with_coordinates.csv")
            df.to_csv('data_address_coordinates.csv',index=False)


