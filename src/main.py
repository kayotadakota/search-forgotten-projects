import asyncio
import aiohttp
import sqlite3

from datetime import date
from requests import Session
from constants import (
    GET_CATALOGUE_HEADERS
)

import logging
logger = logging.getLogger(__name__)

TOTAL_PAGES = None
TITLES_NAMES = []


def init_request(headers):
    url = 'https://api.remanga.org/api/search/catalog/'
    params = {
        'content': 'manga',
        'count': 30,
        'count_chapters_gte': 0,
        'count_chapters_lte': 0,
        'issue_year_gte': 2024,
        'ordering': '-id',
        'page': 1,
        'exclude_types': 4,
        'exclude_types': 5,
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            logger.info('Initial data successfully received.')
            data = response.json()

            global TOTAL_PAGES
            TOTAL_PAGES = data['props']['total_pages']

            for title in data['content']:
                TITLES_NAMES.append(title['dir'])

        else:
            logger.warning(f'Bad status code: {response.status_code}')
    except Exception as ex:
        logger.warning(f'Unexpected exception occured: {ex}')


# async def get_catalogue(session, headers, page):
#     url = 'https://api.remanga.org/api/search/catalog/'
#     params = {
#         'content': 'manga',
#         'count': 30,
#         'count_chapters_gte': 0,
#         'count_chapters_lte': 0,
#         'issue_year_gte': 2024,
#         'ordering': '-id',
#         'page': page,
#         'exclude_types': 4,
#         'exclude_types': 5,
#     }

#     try:
#         async with session.get(url, params=params, headers=headers) as response:
#             if response.status == 200:
#                 logger.info(f'Data from page {params['page']} received.') 
#                 data = await response.json()

#                 for title in data['content']:
#                     TITLES_NAMES.append(title['dir'])

#             else:
#                 logger.warning(f'Bad status code: {response.status}')
#     except Exception as ex:
#         logger.warning(f'Unexpected exception occured: {ex}')


def get_titles_with_expired_immune_date(connection, cursor):
    try:
        logger.info('Extracting data from the database...')
        response = cursor.execute('''
            SELECT title_name FROM Titles WHERE immune_date = date('now');
        ''')
        titles = [title[0] for title in response.fetchall()]

        if not titles:
            logger.info('Titles with expired immune date have not been found.')
            return False
        
        delete_from_db(connection, cursor, titles)
    except Exception as ex:
        logger.warning(f'Unexpected error has occured: {ex}')
    else:
        logger.info(f'{len(titles)} title(s) have been successfully extracted.')
        return titles
    

def delete_from_db(connection, cursor, titles: list[str]) -> None:
    try:
        logger.info('Deleting from database...')
        cursor.executemany(f'''
            DELETE FROM Titles WHERE title_name = ?
        ''', titles)
        connection.commit()
    except Exception as ex:
        logger.warning(f'Unexpected error has occured: {ex}')
    else:
        logger.info(f'{len(titles)} title(s) have been successfully deleted.')


def fetch_all_from_db(cursor) -> dict:
    try:
        logger.info('Extracting data from the database...')
        response = cursor.execute('''
            SELECT title_name FROM Titles;
        ''')
        titles = [title[0] for title in response.fetchall()]
    except Exception as ex:
        logger.warning(f'Unexpected error has occured: {ex}')
    else:
        logger.info(f'{len(titles)} title(s) have been successfully extracted.')
    return dict(zip(titles, range(len(titles))))


def insert_into_db(connection, cursor, data: list[tuple]) -> None:
    try:
        logger.info('Inserting data into database...')
        cursor.executemany('''
            INSERT INTO Titles VALUES(?, ?, ?)
        ''', data)
        connection.commit()
    except Exception as ex:
        logger.warning(f'Unexpected error has occured: {ex}')
    else:
        logger.info(f'{len(data)} new title(s) have been successfully inserted into database.')


def get_title_info(session: Session, headers: dict, title_name: str) -> tuple | bool:
    url = 'https://api.remanga.org/api/titles/'
    # info = {
    #     'title_name': title_name,
    #     'total_bookmarks': 0,
    #     'immune_date': None,
    #     'is_valid': True
    # }

    try:
        response = session.get(f'{url}{title_name}', headers=headers)
        if response.status_code == 200:
            data = response.json()

            if data.get('content'):
                today = date.today()
                target = date.fromisoformat(data.get('content').get('branches')[0].get('immune_date')[:10])

                if today < target:
                    # info['total_bookmarks'] = data.get('content').get('count_bookmarks')
                    # info['immune_date'] = target.isoformat()
                    total_bookmarks = data.get('content').get('count_bookmarks')
                    immune_date = target.isoformat()
                    logger.info(f'Data for {title_name} has been successfully received.')
                else:
                    return False
            else:
                logger.warning(f'{data.get('msg')}')
        else:
            logger.warning(f'Bad status code: {response.status_code}')
    except Exception as ex:
        logger.warning(f'Unexpected error has occured: {ex}')
    else:
      return (title_name, total_bookmarks, immune_date)
        

def get_catalogue(session: Session, headers: dict, page: int) -> list[dict]:
    url = 'https://api.remanga.org/api/search/catalog/'
    params = {
        'content': 'manga',
        'count': 30,
        'count_chapters_gte': 0,
        'count_chapters_lte': 0,
        'issue_year_gte': 2024,
        'ordering': '-id',
        'page': page,
        'exclude_types': 4,
        'exclude_types': 5,
    }
    output_list = []

    try:
        response = session.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()

            if data.get('content'):
                for title in data.get('content'):
                    output_list.append(title.get('dir'))
                logger.info(f'Data from page {page} has been successfully received.')
            else:
                raise Exception(f'{data.get('msg')}')
        else:
            raise Exception(f'Bad status code: {response.status_code}')
    except Exception as ex:
        logger.warning(f'Unexpected exception has occured: {ex}')
    else:
        return output_list


async def main():

    async with aiohttp.ClientSession() as session:

        page = 1
        titles = await get_catalogue(session, GET_CATALOGUE_HEADERS, page)

        tasks = []

        tasks = [get_catalogue(session, GET_CATALOGUE_HEADERS, page) for page in range(2, TOTAL_PAGES + 1)]
        await asyncio.gather(*tasks)

    for title in TITLES_NAMES:
        print(title)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    request_queue = []

    connection = sqlite3.connect('database/titles_info.db')
    cursor = connection.cursor()
    assert cursor.connection, 'Failed to connect to the database.'

    titles = fetch_all_from_db(cursor)

    session = Session()
    titles_info = []
    for_update = []
    flag = True
    
    for page in range(1, 5):
        if flag:
            catalogue = get_catalogue(session, GET_CATALOGUE_HEADERS, page)
            for title in catalogue:

                if title in titles:
                    continue

                result = get_title_info(session, GET_CATALOGUE_HEADERS, title)

                if result:
                    titles_info.append(result)
                else:
                    flag = False
        

    if titles_info:
        insert_into_db(connection, cursor, titles_info)

    print(titles_info)
    print(len(titles_info))
    print(get_titles_with_expired_immune_date(connection, cursor))
    connection.close()
    session.close()

    #init_request(GET_CATALOGUE_HEADERS)
    
    #asyncio.run(main())