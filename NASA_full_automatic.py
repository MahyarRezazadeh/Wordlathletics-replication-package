import aiohttp
import glob
import asyncio
import os
import logging
import pandas as pd
import numpy as np
import aiofiles

from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import math
from lib.utils import create_cookies ,create_pandas_dataframe_and_delete_file_T2,create_pandas_dataframe_and_delete_file_O3,create_pandas_dataframe_and_delete_file_Rainfall,create_pandas_dataframe_and_delete_file_Rainfall2,create_pandas_dataframe_and_delete_file_PRECTOT,create_pandas_dataframe_and_delete_file_PM25,findNearestCoordinatesInKm,create_pandas_dataframe_and_delete_file_T2M

logging.basicConfig(filename='nasa_auto_error.txt',level=logging.ERROR,format='%(asctime)s:%(levelname)s:%(message)s')

async def download_file(session,url,semaphore,log_file):
    async with semaphore:
        try:
            filename = os.path.basename(url)
            print(f'Start downloading {filename}')
            async with session.get(url) as response:
                if response.status == 200:
                    with open('NASA_downloads/'+filename,'ab') as file:
                        while True:
                            chunk = await response.content.read(1024)
                            if not chunk:
                                break
                            file.write(chunk)
                    print(f'Downloaded to NASA_downloads/'+filename)
                    if 'statD_2d_slv_Nx' in filename:
                        create_pandas_dataframe_and_delete_file_T2('NASA_downloads/'+filename,filename.split('.')[2])
                    elif 'inst6_3d_ana_Nv' in filename:
                        create_pandas_dataframe_and_delete_file_O3('NASA_downloads/'+filename,filename.split('.')[2])
                    elif 'FLDAS_NOAH01_C_GL_MA' in filename:
                        create_pandas_dataframe_and_delete_file_Rainfall('NASA_downloads/'+filename,filename.split('.')[1][4:])
                    elif 'GLDAS_NOAH025_M' in filename:
                        create_pandas_dataframe_and_delete_file_Rainfall2('NASA_downloads/'+filename, filename.split('.')[1][1:])
                    elif 'tavgU_2d_flx_Nx' in filename:
                        create_pandas_dataframe_and_delete_file_PRECTOT('NASA_downloads/'+filename,filename.split('.')[2])
                    elif 'tavg1_2d_aer_Nx' in filename:
                        create_pandas_dataframe_and_delete_file_PM25('NASA_downloads/'+filename,filename.split('.')[2])
                    elif 'inst1_2d_asm_Nx' in filename:
                        create_pandas_dataframe_and_delete_file_T2M('NASA_downloads/'+filename,filename.split('.')[2])

                else:
                    raise aiohttp.HttpProcessingError(code=response.status, message="Non-200 status code")        
        except Exception as e:
            print(f"Failed to download {url}: {str(e)}")
            # Log the undownloaded URL with exception details
            async with aiofiles.open(log_file, 'a') as log:
                await log.write(f"{url}\n")
            


async def main(path):
    #  if the size of the downloaded files are small you can use other number like 2 or 4 instead of 1
    semaphore = asyncio.Semaphore(1)
    log_file = "undownloaded_urls.log"
    with open(path,'r') as file:
        url_str = file.read()
    urls = url_str.split('\n')
    async with aiohttp.ClientSession(cookies=cookies) as session:
        tasks = [download_file(session, url, semaphore,log_file) for url in urls]
        await asyncio.gather(*tasks)


def process_date(date, country_df, folder_to_save, folder_to_read):
    try:
        df_path = f"{folder_to_read}/{date}.csv"
        nasa_params_for_date_df = pd.read_csv(df_path)
        nasa_params_for_date_df.loc[nasa_params_for_date_df[np.abs(nasa_params_for_date_df['lon'])<0.0001].index,'lon'] = 0
        nasa_params_for_date_df.loc[nasa_params_for_date_df[np.abs(nasa_params_for_date_df['lat'])<0.0001].index,'lat'] = 0
        unique_cities = country_df.drop_duplicates(subset=['NASA_lat','NASA_lon'])
        if folder_to_read == 'T2':
            country_df['T2MMEAN'] = None
            country_df['TPRECMAX'] = None
            for index,row in unique_cities.iterrows():
                try:
                    country_df.loc[(country_df['NASA_lat'] == row['NASA_lat']) & (country_df['NASA_lon'] == row['NASA_lon']),'T2MMEAN'] = nasa_params_for_date_df.loc[(nasa_params_for_date_df['lat']==row['NASA_lat']) & (nasa_params_for_date_df['lon']==row['NASA_lon']),'T2MMEAN'].iloc[0]
                    country_df.loc[(country_df['NASA_lat'] == row['NASA_lat']) & (country_df['NASA_lon'] == row['NASA_lon']),'TPRECMAX'] = nasa_params_for_date_df.loc[(nasa_params_for_date_df['lat']==row['NASA_lat']) & (nasa_params_for_date_df['lon']==row['NASA_lon']),'TPRECMAX'].iloc[0]
                except:
                    logging.error(f'error in computing T2 and extracting tempretures for all cities for date={date}')
            # country_df = country_df.groupby('NAME_3').agg({'admin':'first','GID_3':'first','lon':'mean','lat':'mean','NASA_lat':'mean','NASA_lon':'mean','distance':'mean','T2MMEAN':'mean','TPRECMAX':'mean','_ID':'first'})
        elif folder_to_read=='RainFallMonthly':
            country_df['PRECTOT'] = None
            for index,row in unique_cities.iterrows():
                try:
                    country_df.loc[(country_df['NASA_lat'] == row['NASA_lat']) & (country_df['NASA_lon'] == row['NASA_lon']),'PRECTOT'] = nasa_params_for_date_df.loc[(nasa_params_for_date_df['lat']==row['NASA_lat']) & (nasa_params_for_date_df['lon']==row['NASA_lon']),'PRECTOT'].iloc[0]
                except:
                    logging.error(f'error in computing PRECTOT and extracting tempretures for all cities for date={date}')
            # country_df = country_df.groupby('NAME_3').agg({'admin':'first','GID_3':'first','lon':'mean','lat':'mean','NASA_lat':'mean','NASA_lon':'mean','distance':'mean','PRECTOT':'mean','_ID':'first'})
        elif folder_to_read=='PM25':
            country_df['PM25'] = None
            for index,row in unique_cities.iterrows():
                try:
                    country_df.loc[(country_df['NASA_lat'] == row['NASA_lat']) & (country_df['NASA_lon'] == row['NASA_lon']),'PM25'] = nasa_params_for_date_df.loc[(nasa_params_for_date_df['lat']==row['NASA_lat']) & (nasa_params_for_date_df['lon']==row['NASA_lon']),'PM25'].iloc[0]
                except:
                    logging.error(f'error in computing PM25 and extracting tempretures for all cities for date={date}')
            # country_df = country_df.groupby('NAME_3').agg({'admin':'first','GID_3':'first','lon':'mean','lat':'mean','NASA_lat':'mean','NASA_lon':'mean','distance':'mean','_ID':'first','PM25':'mean'})
        elif folder_to_read == 'O3':
            O3_params = ['PS', 'DELP', 'T', 'U', 'V', 'QV', 'O3']
            for parameter in O3_params:
                country_df[parameter] = None
            for index,row in unique_cities.iterrows():
                try:
                    for parameter in O3_params:
                        country_df.loc[(country_df['NASA_lat'] == row['NASA_lat']) & (country_df['NASA_lon'] == row['NASA_lon']),parameter] = nasa_params_for_date_df.loc[(nasa_params_for_date_df['lat']==row['NASA_lat']) & (nasa_params_for_date_df['lon']==row['NASA_lon']),parameter].iloc[0]
                except:
                    logging.error(f'error in computing O3 and extracting tempretures for all cities for date={date}')
            # grouping_conf = {'admin':'first','GID_3':'first','lon':'mean','lat':'mean','NASA_lat':'mean','NASA_lon':'mean','distance':'mean','_ID':'first'}

            # for parameter in O3_params:
                # grouping_conf[parameter] = 'mean'
        elif folder_to_read == 'T2M':
            T2M_parameter = ['T10M','T2M','QV2M','TS']
            for parameter in T2M_parameter:
                country_df[parameter] = None
            for index,row in unique_cities.iterrows():
                try:
                    for parameter in T2M_parameter:
                        country_df.loc[(country_df['NASA_lat'] == row['NASA_lat']) & (country_df['NASA_lon'] == row['NASA_lon']),parameter] = nasa_params_for_date_df.loc[(nasa_params_for_date_df['lat']==row['NASA_lat']) & (nasa_params_for_date_df['lon']==row['NASA_lon']),parameter].iloc[0]
                except:
                    logging.error(f'error in computing T2M and extracting tempretures for all cities for date={date}',exc_info=True)
            grouping_conf = {'admin':'first','GID_3':'first','lon':'mean','lat':'mean','NASA_lat':'mean','NASA_lon':'mean','distance':'mean','_ID':'first','NAME_0':'first'}
            for parameter in T2M_parameter:
                grouping_conf[parameter] = 'mean'
            country_df = country_df.groupby('NAME_3').agg(grouping_conf)

        country_df.to_csv(f'{folder_to_save}/{date}.csv')
    except Exception as e:
        print(str(e))

     



def merge(country_df:pd.DataFrame,folder_to_save:str,folder_to_read:str):
    if FREQ == 'MS':
        date_range = list(pd.date_range(START_DATE, END_DATE,freq=FREQ).strftime('%Y%m'))
    elif FREQ == 'D':
        date_range = list(pd.date_range(START_DATE, END_DATE).strftime('%Y%m%d'))

    process_specific_period = partial(process_date, country_df=country_df,folder_to_save=folder_to_save,folder_to_read=folder_to_read)

    # Use ProcessPoolExecutor to run the processing in parallel
    with ProcessPoolExecutor(max_workers=4) as executor:
        # Process the dates in parallel
        results = executor.map(process_specific_period, date_range)



def prepare_country(fname:str,parameter:str) -> pd.DataFrame:
    country_df = pd.read_csv(f'country/{fname}.csv',index_col=0)
    country_df = country_df.rename(columns={'lng':'lon'})
    country_df.dropna(subset=['lat','lon'],inplace=True)
    country_df['NASA_lat'] = None
    country_df['NASA_lon'] = None
    country_df['distance'] = None
    country_df[['NASA_lat','NASA_lon','distance']] = country_df.apply(findNearestCoordinatesInKm,axis=1,args=([],[]))
    country_df.dropna(subset=['lat','lon'],inplace=True)

    country_df.to_csv(f'country/{fname}_with_nasa_{parameter}.csv')
    return country_df




if __name__ == '__main__':

    if not os.path.exists('NASA_downloads'):
        os.makedirs('NASA_downloads')

    # Step 1: make sure the cookies are still valid. cookies need to updated every one week.
    cookies_text = 'urs_guid_ops=4b121d99-271d-4a52-801f-9e82865336c0; _ga_ZN04B6H2T9=GS1.1.1717754255.2.0.1717754255.0.0.0; _ga_7D3CVZM5MK=GS1.1.1718008900.7.0.1718008900.0.0.0; _ga_RLWG0EH56X=GS1.1.1718172803.3.1.1718176774.0.0.0; 817101319027121419341118516615=s%3AVx16QmmxzSz9-WEw-6_r7vh5sEqH_gFI.BeLXfcFOZrOm1NYM4ArvMya0xMq2c05paN0%2FSwouWlo; _gid=GA1.2.1091977598.1720431104; _ga_WXLRFJLP5B=GS1.1.1720431102.9.1.1720431581.0.0.0; _gat_UA-62340125-2=1; _gat_UA-112998278-1=1; _ga_T0WYSFJPBT=GS1.1.1720443378.6.0.1720443380.0.0.0; _ga_CSLL4ZEK4L=GS1.1.1720443382.19.0.1720443382.0.0.0; _ga=GA1.1.1133836325.1717747734; urs_user_already_logged=yes; asf-urs=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1cnMtdXNlci1pZCI6InNhbWFubWFkYW5pNzEiLCJmaXJzdF9uYW1lIjoic2FtYW4iLCJsYXN0X25hbWUiOiJtYWRhbmkiLCJlbWFpbCI6InNhbWFuLm1hZGFuaTcxQGdtYWlsLmNvbSIsInVycy1hY2Nlc3MtdG9rZW4iOiJleUowZVhBaU9pSktWMVFpTENKdmNtbG5hVzRpT2lKRllYSjBhR1JoZEdFZ1RHOW5hVzRpTENKemFXY2lPaUpsWkd4cWQzUndkV0pyWlhsZmIzQnpJaXdpWVd4bklqb2lVbE15TlRZaWZRLmV5SjBlWEJsSWpvaVQwRjFkR2dpTENKamJHbGxiblJmYVdRaU9pSmxNbGRXYXpoUWR6WjNaV1ZNVlV0YVdVOTRkbFJSSWl3aVpYaHdJam94TnpJek1ETTFNemczTENKcFlYUWlPakUzTWpBME5ETXpPRGNzSW1semN5STZJa1ZoY25Sb1pHRjBZU0JNYjJkcGJpSXNJblZwWkNJNkluTmhiV0Z1YldGa1lXNXBOekVpZlEuVDE0MEtQZUdWRll0WHRsR1FvN3JobFpBbUpReEJuOXdrcUMwMGdILUROM2RCOXgwMDMxRU5vdEJDM3U4ZGQ4TEVuWmREOUpmQ09NR0VTYWdWMVNWZWxqZUtDWklxWDBTM3VnSTBJYXNtV1lVbjZjN0ZjcWdxRVFVcXlLSWJIME8yZk0wSGJvMTZWZFVKaDJxamRwblJ0Q1BFR280bTVIZ294SDhteXd0NV83VXhsamxZbVBpM3NldWwxMlJqWlZTLVpBZ041dlhEMmc5clVlYmVidlRCUXdQazZRdURuWE9KNV9TUkNkajhhSmdoUTFONG1wQXhkbnVxYlphWmNrVnAtSElXMmdlSGtLSjlpVENhajY0UllkZGlQbGowcWFRVE9SNEVYb1l6NWxSNWt1UFpLYW5UenVJZUpNdExfQldjOXAzNXNNMW5ya1Zlcld0Z0dNZE53IiwidXJzLWdyb3VwcyI6W10sImlhdCI6MTcyMDQ0MzM4NywiZXhwIjoxNzIxMDQ4MTg3fQ.S-2ufXt5RlKHsl-2PImU2COmuPRy1jNVrLC0onGOetY9ElCNWhfgICJR5QjtoZZknm9i3Ju7cyfiwrjdMDK68gOHgClRzewFswpd1ZNkEaEP2PQPJ_QeISR292eZLg85HQUfw6lEph2Q1QV9NiDuB3dW7nBFe4--T2OQ2hfRsaEEYsE-7a4AOgt-NOVeM8H-ycDH1kIt976XtSP3HxF0LRFSn2ecAokWQy1rVagvW-Astwd5hm8ik3UFRMzoUvYrdIHYcGLKVIMdOLN7ERcn_4hcaLXMXnJGerxOJwymrCFutK_VpMsGY_WuBT7Str46pZ-DW9ADn7diLgp2V_1pqruEDqJe6WdHIFIU2GYvTLsqThIN3wsOC5ZAY_z7o-zaaxzcJnjCa-Qmk_VGJMtZnC23vgxzm29aOrKEjHq2TMX9PisGivc7ldLPFshi8s0oPgsNFK-07aWjqaw0MP_l5gwf6c19Z3ov8WAhi5GQr58zoe6LFDSFMmp7eLoSRsEK-RwRb9rAj2c9iW1pt1DPMHTSQVQbu4Ewsdvq663VSiBrAfW-5vW4yS125HTkZfmURI9Os7geIAdhFpjJT1LTJJm8rAmWBJysBo8Enq5WpO3l2pcaSUhev-1mVv-rZBlTRF1S6znurPTXlwBavak76C6Bn4MsASOaL3rWqHsD9wQ'
    cookies = create_cookies(cookies_text)


    # Step 2: Check the period and timeframe
    START_DATE = '2008-01-01'
    END_DATE = '2016-12-31'
    FREQ = 'D' # for mountly MS (monthly start) and for daily use 'D'


    # Step 3: provide related NASA file. Make sure that the dates of the links are within START_DATE and END_DATE in above
    # provide the file name with your desire period links in the file_name variable
    file_name = 'subset_M2I1NXASM_5.12.4_20240708_095054_.txt'

    if 'M2TUNXFLX_5.12.4' in file_name:
        parameter = 'RainFallMonthly'
    elif 'M2T1NXAER_5.12.4' in file_name:
        parameter = 'PM25'
    elif 'M2I6NVANA_5.12.4' in file_name:
        parameter = 'O3'
    elif 'M2SDNXSLV_5.12.4' in file_name:
        parameter = 'T2'
    elif 'M2I1NXASM_5.12.4' in file_name:
        parameter = 'T2M'
    # Ensure the directory exists
    if not os.path.exists(parameter):
        os.mkdir(parameter)


    #  This array should be replaced
    path = f'NASA/{file_name}'

    # asyncio.run(main(path))

    #  this is the file of the cities with lat,lon, and city name. this three columns are mendatory
    for fname in ['ethiopia']:
        try:
            # folder_to_save = input('In which folder do you want to save the result? \n')
            folder_to_save = f'M2I1NXASM_{fname}'
            if not os.path.exists(folder_to_save):
                os.makedirs(folder_to_save)

            if not os.path.exists(f'country/{fname}_with_nasa_{parameter}.csv'):
                country_df = prepare_country(fname,parameter)
            else:
                country_df = pd.read_csv(f'country/{fname}_with_nasa_{parameter}.csv',index_col=0)

            print(f'start merging {fname}')
            merge(country_df,folder_to_save,folder_to_read=parameter)
            paths = glob.glob(f'{folder_to_save}/*.csv')
            all_df = pd.DataFrame()
            all_df = []
            for path in tqdm(paths):
                df = pd.read_csv(path)
                df['date'] = path.split('/')[1].split('.')[0]
                all_df.append(df)

            all_df = pd.concat(all_df,axis=0)
            print('start saving csv file')
            all_df.to_csv(f'{fname}_{parameter}.csv')
            print(f'end result csv file saved in the {fname}_{parameter}.csv')
            print('start saving dta file')
            all_df.to_stata(f'{fname}_{parameter}.dta')
            print(f'end result dta file saved in the {fname}_{parameter}.dta')
        except Exception as e:
            print(f'error in merging and post processing of the {fname} file: ' + str(e))
            logging.error(f'error in merging and post processing of the {fname} file: ' + str(e))
