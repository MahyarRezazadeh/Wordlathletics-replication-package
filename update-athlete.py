import requests
import pandas as pd
import numpy as np
import json
import datetime
import os
import glob
import asyncio
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from tqdm import tqdm
import multiprocessing


async def fetch_data(session: ClientSession, url) -> str:
    async with session.get(url, ssl=False) as result:
        return await result.text()


def create_till_today_dataframe(id: int, today: str) -> pd.DataFrame:
    # url to get the data of all the competitions --> the data is like this url = https://worldathletics.org/competition/calendar-results?hideCompetitionsWithNoResults=true
    # url: str =  'https://5ussliaxonedrgkljafudqdslu.appsync-api.eu-west-1.amazonaws.com/graphql'
    url: str =  'https://graphql-prod-4670.prod.aws.worldathletics.org/graphql'
    # the configure file for getting results from graphql
    with open('payload.json', 'r') as file:
        payload = json.load(file)

    counter: int = 0
    resultDataFrame: pd.DataFrame = pd.DataFrame()
    resultDataFrame_new: pd.DataFrame = pd.DataFrame()

    while True:
        payload['variables']['offset'] = counter
        payload['variables']['limit'] = 200
        print(payload['variables']['offset'], payload['variables']['limit'])

        headers = {
            "Authority": "graphql-prod-4670.prod.aws.worldathletics.org",
            "Refere": "https://worldathletics.org/",
            "X-Amz-User-Agent": "aws-amplify/3.0.2",
            "X-Api-Key": "da2-sbkbfivktbgujopombl7qxelhq"
        }

        req = requests.post(url, json=payload, headers=headers)
        data = req.json()
        resultDataFrame_new = pd.DataFrame(
            data['data']['getCalendarEvents']['results'])
        resultDataFrame = pd.concat(
            [resultDataFrame, resultDataFrame_new], axis=0, ignore_index=True)
        if id in resultDataFrame['id'].values:
            break
        counter += 200

    today_date_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'start saving data to {today} directory.')
    resultDataFrame.to_csv(f'{today}/till_{today_date_str}.csv')
    return resultDataFrame


def remove_duplicates(resultDataFrame: pd.DataFrame) -> pd.DataFrame:
    df = pd.read_csv('updated_data.csv')
    df_id = set(df['id'])
    resultDataFrame_id = set(resultDataFrame['id'])
    unique_id = resultDataFrame_id - df_id
    resultDataFrame.set_index('id', drop=True, inplace=True)
    df = resultDataFrame.loc[list(unique_id)]
    df.reset_index(inplace=True)
    return df


async def create_json_files(resultDataFrame: pd.DataFrame, today) -> None:

    BASE_URL = 'https://worldathletics.org/competition/calendar-results/results/'

    length = resultDataFrame.shape[0]
    step = 20
    # df_all = pd.DataFrame()

    # counter = 0
    # errorList = []
    async with ClientSession() as session:
        counter = 0
        for i in range(0, length, step):
            try:
                urls = [BASE_URL + str(i)
                        for i in resultDataFrame['id'][i:i+step]]

                ids = [str(i) for i in resultDataFrame['id'][i:i+step]]
                requests = [fetch_data(session, url) for url in urls]
                bodies = await asyncio.gather(*requests)
                c = 0
                for body in bodies:
                    # try:
                    soup = BeautifulSoup(body, 'html.parser')
                    data = soup.find('script', id='__NEXT_DATA__')
                    # newData = json.loads(data.get_text())
                    with open(f'json_{today}/data_{counter}_{ids[c]}.json', 'w') as file:
                        file.write(data.get_text())
                    c += 1
                    counter += 1
            except Exception as e:
                print(str(e))
                # c += 1
                counter += 1


def get_summary_df(df_all, events, dateRange, endDate, name, startDate, venue, rankingCategory, eventTitle, path):
    for event_ in events:
        event = event_['event']
        eventId = event_['eventId']
        gender = event_['gender']
        isRelay = event_['isRelay']
        perResultWind = event_['perResultWind']
        withWind = event_['withWind']

        for summary_ in event_['summary']:
            df = pd.json_normalize(summary_)
            df['rankingCategory'] = rankingCategory
            df['eventTitle'] = eventTitle
            df['event'] = event
            df['eventId'] = eventId
            df['gender'] = gender
            df['perResultWind'] = perResultWind
            df['withWind'] = withWind
            df['date'] = None
            df['day'] = None
            df['race'] = None
            df['raceId'] = None
            df['startList'] = None
            df['id'] = path.split('/')[-1].split('_')[-1][:-5]
            df['name'] = name
            df['venue'] = venue
            df['startDate'] = startDate
            df['endDate'] = endDate
            df['dateRange'] = dateRange
            df['country'] = venue[venue.find(
                '(')+1:venue.find(')')]
            df_all = pd.concat(
                [df_all, df], axis=0, ignore_index=True)
    return df_all


def get_races_df(df_all, events, dateRange, endDate, name, startDate, venue, rankingCategory, eventTitle, path):
    for event_ in events:
        event = event_['event']
        eventId = event_['eventId']
        gender = event_['gender']
        isRelay = event_['isRelay']
        perResultWind = event_['perResultWind']
        withWind = event_['withWind']
        for race_ in event_['races']:
            date = race_['date']
            day = race_['day']
            race = race_['race']
            raceId = race_['raceId']
            raceNumber = race_['raceNumber']
            startList = race_['startList']
            wind = race_['wind']
            for result_ in race_['results']:
                df = pd.json_normalize(result_)
                df['wind2'] = wind
                df['rankingCategory'] = rankingCategory
                df['eventTitle'] = eventTitle
                df['event'] = event
                df['eventId'] = eventId
                df['gender'] = gender
                df['perResultWind'] = perResultWind
                df['withWind'] = withWind
                # df['summary'] = summary
                df['date'] = date
                df['day'] = day
                df['race'] = race
                df['raceId'] = raceId
                df['raceNumber'] = raceNumber
                df['startList'] = startList
                df['id'] = path.split('/')[-1].split('_')[-1][:-5]
                df['name'] = name
                df['venue'] = venue
                df['startDate'] = startDate
                df['endDate'] = endDate
                df['dateRange'] = dateRange
                df['country'] = venue[venue.find(
                    '(')+1:venue.find(')')]
                df_all = pd.concat(
                    [df_all, df], axis=0, ignore_index=True)

    return df_all


def create_dataframe_from_json(path):
    try:
        today = datetime.datetime.now().strftime('%Y-%m-%d')

        counter = 0

        df_all = pd.DataFrame()

        with open(path, 'r') as file:
            newData = json.load(file)
        competition = newData['props']['pageProps']['calendarEventsResults']['competition']
        dateRange = competition['dateRange']
        endDate = competition['endDate']
        name = competition['name']
        startDate = competition['startDate']
        venue = competition['venue']

        for eventTitle_ in newData['props']['pageProps']['calendarEventsResults']['eventTitles']:
            rankingCategory = eventTitle_['rankingCategory']
            eventTitle = eventTitle_['eventTitle']

            if (eventTitle == 'Combined Events'):
                df_all = get_summary_df(df_all, eventTitle_[
                    'events'], dateRange, endDate, name, startDate, venue, rankingCategory, eventTitle, path)

                try:
                    df_all = get_races_df(df_all, eventTitle_[
                        'events'], dateRange, endDate, name, startDate, venue, rankingCategory, eventTitle, path)
                except:
                    print('not have races')

            else:
                df_all = get_races_df(df_all, eventTitle_[
                    'events'], dateRange, endDate, name, startDate, venue, rankingCategory, eventTitle, path)
        df_all.to_csv(
            f"data_{today}/all_{path.split('/')[-1].split('_')[-1][:-5]}.csv")
    except Exception as e:
        with open('error.txt', 'w') as file:
            file.write(path + '\n')


def stick_chunk(today: str):
    columns = ['id', 'mark', 'nationality', 'place', 'points', 'qualified', 'records', 'wind', 'wind2', 'remark', 'details', 'competitor.name', 'competitor.birthDate', 'rankingCategory',
               'eventTitle', 'event', 'gender', 'date', 'day', 'race', 'raceNumber', 'name', 'venue', 'startDate', 'endDate', 'dateRange', 'country', 'details']

    paths = glob.glob(f'data_{today}/*.csv')
    for i in range(0, len(paths)+10, 1000):
        all_df = pd.DataFrame()

        for path in tqdm(paths[i:i+1000]):
            df = pd.read_csv(path)
            df = df[columns]

            all_df = pd.concat([all_df, df], ignore_index=True)

        all_df.to_csv(f'./result/aggregated_data_{today}_{i}.csv')


def stick_all(today: str) -> pd.DataFrame:
    columns = ['id', 'mark', 'nationality', 'place', 'points', 'qualified', 'records', 'wind', 'wind2', 'remark', 'details', 'competitor.name', 'competitor.birthDate', 'rankingCategory',
               'eventTitle', 'event', 'gender', 'date', 'day', 'race', 'raceNumber', 'name', 'venue', 'startDate', 'endDate', 'dateRange', 'country', 'details']

    paths = glob.glob(f'./result/aggregated_data_{today}_*.csv')
    all_df = pd.DataFrame()
    for path in paths:
        df = pd.read_csv(path)
        df = df[columns]
        all_df = pd.concat([all_df, df], ignore_index=True)
    all_df.to_csv(f'./result/raw_data.csv')
    return all_df


def add_competition_group(df: pd.DataFrame, details_df: pd.DataFrame, today):
    fieldName = 'competitionGroup'
    details_df[fieldName] = ''
    for i, row in tqdm(df.iterrows()):
        details_df.loc[details_df[details_df['id'] == row['id']].index,
                       fieldName] = row[fieldName]

    details_df['startDate'] = pd.to_datetime(
        details_df['startDate'], format='%Y-%m-%d')
    details_df['endDate'] = pd.to_datetime(
        details_df['endDate'], format='%Y-%m-%d')

    details_df.sort_values(by=['startDate', 'endDate'],
                           ascending=False, inplace=True)

    details_df.to_csv(
        f'./result/data_with_competition_group_{today}.csv', index=False)
    return details_df


def merge_with_old_data(today):
    new_data: pd.DataFrame = pd.read_csv(
        f'result/data_with_competition_group_{today}.csv')
    old_data: pd.DataFrame = pd.read_csv('updated_data.csv')
    updated_data = pd.concat([old_data, new_data], ignore_index=True)
    updated_data.sort_values(by=['startDate', 'endDate'],
                             ascending=False, inplace=True)
    updated_data.drop_duplicates(inplace=True)
    updated_data.to_csv(f'updated_data.csv', index=False)
    return updated_data


async def main():
    # to get the last id that has scrapped in previous run
    df = pd.read_csv('updated_data.csv', nrows=1)
    id: np.float64 = df['id'][0]
    id: int = int(id)
    print('last id that have got: ', id)
    today = datetime.datetime.now().strftime('%Y-%m-%d')

    if not os.path.exists(f'{today}'):
        print(f'create {today} directory')
        os.mkdir(f'{today}')
    if not os.path.exists(f'data_{today}'):
        print(f'create data_{today} directory')
        os.mkdir(f'data_{today}')
    if not os.path.exists(f'json_{today}'):
        print(f'create json{today} directory')
        os.mkdir(f'json_{today}')
    if not os.path.exists('result'):
        print(f'create result directory')
        os.mkdir('result')

    print('start getting data from world athlic database ... ')
    result_df = create_till_today_dataframe(id, today)

    print('start removing duplicated data')
    result_df = remove_duplicates(result_df)
    print('start async geting json data of each event')
    await create_json_files(result_df, today)
    paths = glob.glob(f'./json_{today}/*.json')
    errorList = []
    df_all = pd.DataFrame()
    print('start multiprocessing converting json to dataframe for each event')
    with multiprocessing.Pool() as pool:
        pool.map(create_dataframe_from_json, paths)

    print('stiching data to eachother to create 100,000 rows csv files')
    stick_chunk(today)
    print('stiching above data to eachother to create main data')
    all_details_df = stick_all(today)
    print('add competition groups to data')
    details_df = add_competition_group(result_df, all_details_df, today)
    print('merge data to old data to create new update data')
    updated_data_whole = merge_with_old_data(today)

if __name__ == '__main__':
    asyncio.run(main())
    # # to get the last id that has scrapped in previous run
    # df = pd.read_csv('updated_data.csv', nrows=1)
    # id: np.float64 = df['id'][0]
    # id: int = int(id)
    # print('last id that have got: ', id)
    # today = datetime.datetime.now().strftime('%Y-%m-%d')

    # if not os.path.exists(f'{today}'):
    #     print(f'create {today} directory')
    #     os.mkdir(f'{today}')
    # if not os.path.exists(f'data_{today}'):
    #     print(f'create data_{today} directory')
    #     os.mkdir(f'data_{today}')
    # if not os.path.exists(f'json{today}'):
    #     print(f'create json{today} directory')
    #     os.mkdir(f'json{today}')
    # if not os.path.exists('result'):
    #     print(f'create result directory')
    #     os.mkdir('result')

    # print('start getting data from world athlic database ... ')
    # result_df = create_till_today_dataframe(id, today)

    # print('start removing duplicated data')
    # result_df = remove_duplicates(result_df)
    # print('start async geting json data of each event')
    # asyncio.run(create_json_files(result_df, today))
    # paths = glob.glob(f'./json_{today}/*.json')
    # errorList = []
    # df_all = pd.DataFrame()
    # print('start multiprocessing converting json to dataframe for each event')
    # with multiprocessing.Pool() as pool:
    #     pool.map(create_dataframe_from_json, paths)

    # print('stiching data to eachother to create 100,000 rows csv files')
    # stick_chunk(today)
    # print('stiching above data to eachother to create main data')
    # all_details_df = stick_all(today)
    # print('add competition groups to data')
    # details_df = add_competition_group(result_df, all_details_df)
    # print('merge data to old data to create new update data')
    # updated_data_whole = merge_with_old_data(details_df)
