import sqlite3

from datetime import date
from requests import Session
from constants import (
    GET_CATALOGUE_HEADERS
)

import logging
logger = logging.getLogger(__name__)


def get_titles_with_expired_immune_date(connection, cursor) -> list[str] | bool:
    try:
        logger.info('Extracting data from the database...')
        response = cursor.execute('''
            SELECT title_name FROM Titles WHERE immune_date < date('now');
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
        for title in titles:
            cursor.execute(f'''
                DELETE FROM Titles WHERE title_name = '{title}';
            ''')
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

    try:
        response = session.get(f'{url}{title_name}', headers=headers)
        if response.status_code == 200:
            data = response.json()

            if data.get('content'):
                today = date.today()
                target = date.fromisoformat(data.get('content').get('branches')[0].get('immune_date')[:10])

                if today <= target:
                    total_bookmarks = data.get('content').get('count_bookmarks')
                    immune_date = target.isoformat()
                    logger.info(f'Data for {title_name} has been successfully received.')
                    return (title_name, total_bookmarks, immune_date)
                else:
                    return False
            else:
                logger.warning(f'{data.get('msg')}')
        else:
            logger.warning(f'Bad status code: {response.status_code}')
    except Exception as ex:
        logger.warning(f'Unexpected error has occured: {ex}')
        

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


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    connection = sqlite3.connect('database/titles_info.db')
    cursor = connection.cursor()
    assert cursor.connection, 'Failed to connect to the database.'

    tracked_titles = fetch_all_from_db(cursor)
    debut = get_titles_with_expired_immune_date(connection, cursor)
    worth_to_take = []

    session = Session()
    titles_info = []
    flag = True
    
    for page in range(1, 5):
        if flag:
            catalogue = get_catalogue(session, GET_CATALOGUE_HEADERS, page)
            for title in catalogue:

                if title in tracked_titles:
                    continue

                result = get_title_info(session, GET_CATALOGUE_HEADERS, title)

                if result:
                    titles_info.append(result)
                else:
                    flag = False
        

    if titles_info:
        insert_into_db(connection, cursor, titles_info)

    if debut:
        for title in debut:
            info = get_title_info(session, GET_CATALOGUE_HEADERS, title)

            if info[1] > 500:
                # Add title's name to the output if the count of bookmarks is greater than 500 
                worth_to_take.append(info[0])

    connection.close()
    session.close()

    if worth_to_take:
        print(*worth_to_take, sep='\n')
    
    else:
        print('No titles worth taking have been found.')
    
