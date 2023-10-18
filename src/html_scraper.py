import argparse
import asyncio
import re
from glob import glob
from os import makedirs
from os.path import abspath, basename, dirname, getmtime, join
from string import Template
from urllib.parse import urljoin

import aiofiles
import httpx
from bs4 import BeautifulSoup, SoupStrainer
from httpx import ConnectError, ReadError, ReadTimeout, RemoteProtocolError
from tqdm import tqdm
from tqdm.asyncio import tqdm as tqdm_asyncio

PROJECT_DIR = dirname(dirname(abspath(__file__)))
BASE_URL = 'https://www.goodreads.com'
URL = urljoin(BASE_URL, 'review/list/$user_id')
MAX_ASYNC_TASKS = 512


def get_latest_user_id(html_dir: str) -> int:
    files = glob(join(html_dir, '*'))
    latest_file = max(files, key=getmtime)
    return int(re.search(r'user_(\d+).html', basename(latest_file)).group(1))

parser = argparse.ArgumentParser(description='Goodreads HTML Scraper')
parser.add_argument('--folder', type=str, help='Folder to save HTML files', required=True)
parser.add_argument('--user_id_start', type=int, help='From User ID')
parser.add_argument('--user_id_end', type=int, help='To User ID')

args = parser.parse_args()
HTML_DIR = join(PROJECT_DIR, args.folder)
USER_ID_START = args.user_id_start or get_latest_user_id(html_dir=HTML_DIR)
USER_ID_END = args.user_id_end or 999999


async def save_html(user_id: int, response: httpx.Response) -> None:
    if response.url == 'https://www.goodreads.com/' and response.history[0].status_code == 301:
        return
    if 'This Profile Is Restricted to Goodreads Users' in response.text:
        return

    page = response.request.url.params.get('page')
    if not page:
        pass
    file_path = join(HTML_DIR, f'user_{user_id}_{page}.html')
    async with aiofiles.open(file_path, mode='w', encoding='utf-8') as html_file:
        await html_file.write(response.text)


async def get_request(client: httpx.AsyncClient, user_id: int,
                      page_number: int = 1, attempt: int = 1) -> httpx.Response:
    try:
        url = Template(URL).substitute(user_id=user_id)
        params = {'page': page_number, 'per_page': '100',
                  'sort': 'rating', 'utf8': '^%^E2^%^9C^%^93'}
        response = await client.get(url=url, params=params, timeout=60, follow_redirects=True)
    except (ReadTimeout, ConnectError, RemoteProtocolError, ReadError) as exc:
        if attempt >= (20 if isinstance(exc, httpx.ReadTimeout) else 5):
            raise exc
        await asyncio.sleep(1)
        return await get_request(client=client, user_id=user_id,
                                 page_number=page_number, attempt=attempt + 1)
    return response


async def process_requests(client: httpx.AsyncClient,
                           user_id: int, last_page: int) -> list[httpx.Response]:
    page_responses = []

    page_tasks = [get_request(client, user_id, page) for page in range(2, last_page + 1)]
    for i in range(0, len(page_tasks), MAX_ASYNC_TASKS):
        batch_end = min(i + MAX_ASYNC_TASKS, len(page_tasks))
        page_responses.extend(await tqdm_asyncio.gather(*page_tasks[i:batch_end], position=1,
                                                        leave=False, desc='Async Requests'))

    return page_responses


async def process_html_files(user_id: int, page_responses: list[httpx.Response]) -> None:
    save_tasks = [save_html(user_id=user_id, response=response) for response in page_responses]
    for i in range(0, len(save_tasks), MAX_ASYNC_TASKS):
        batch_end = min(i + MAX_ASYNC_TASKS, len(save_tasks))
        await tqdm_asyncio.gather(*save_tasks[i:batch_end],
                                  position=1, leave=False, desc='Saving HTML')


async def crawl_user(client: httpx.AsyncClient, user_id: int) -> None:
    response = await get_request(client, user_id)
    if response.status_code == 301:
        return

    pagination = BeautifulSoup(response.text, 'html.parser',
                               parse_only=SoupStrainer('div', attrs={'id': 'reviewPagination'}))
    if page_elements := pagination.find_all('a'):
        last_page = int(page_elements[-2].text)
        page_responses = await process_requests(client, user_id, last_page)
        await process_html_files(user_id, page_responses)
    else:
        await save_html(user_id=user_id, response=response)


async def main() -> None:
    makedirs(HTML_DIR, exist_ok=True)

    print(f'Scraping from User ID {USER_ID_START} to {USER_ID_END}\n'
          f'Saving HTML files to {HTML_DIR}')

    async with httpx.AsyncClient() as client:
        for user_id in tqdm(range(USER_ID_START, USER_ID_END + 1), position=0, desc='Users', colour='CYAN'):
            await crawl_user(client, user_id)


if __name__ == '__main__':
    asyncio.run(main())
