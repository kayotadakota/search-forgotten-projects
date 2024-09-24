import requests
import asyncio
import aiohttp

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


async def get_catalogue(session, headers, page):
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

    try:
        async with session.get(url, params=params, headers=headers) as response:
            if response.status == 200:
                logger.info(f'Data from page {params['page']} received.') 
                data = await response.json()

                for title in data['content']:
                    TITLES_NAMES.append(title['dir'])

            else:
                logger.warning(f'Bad status code: {response.status}')
    except Exception as ex:
        logger.warning(f'Unexpected exception occured: {ex}')


async def main():

    async with aiohttp.ClientSession() as session:
        tasks = [get_catalogue(session, GET_CATALOGUE_HEADERS, page) for page in range(2, TOTAL_PAGES + 1)]
        await asyncio.gather(*tasks)

    for title in TITLES_NAMES:
        print(title)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    init_request(GET_CATALOGUE_HEADERS)
    
    asyncio.run(main())