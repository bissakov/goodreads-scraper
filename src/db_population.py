import argparse
import asyncio
import os
import re
import subprocess
from datetime import datetime
from itertools import batched
from typing import Optional, Union

import aiofiles
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from src.models import Author, Base, Book, File, Rating, User

Record = Union[Author, Book, File, Rating, User]

DB_FILE = r'C:\Home\Work\graduate-work\book-scraper\goodreads.db'
RATINGS = {'it was amazing': 5,
           'really liked it': 4,
           'liked it': 3,
           'it was ok': 2,
           'did not like it': 1}


parser = argparse.ArgumentParser(description='Goodreads HTML Scraper')
parser.add_argument('--folder', type=str, help='Folder to save HTML files', required=True)
parser.add_argument('--user_id_start', type=int, help='From User ID')
parser.add_argument('--user_id_end', type=int, help='To User ID')

args = parser.parse_args()
HTML_DIR: str = args.folder


async def read_html_file(file_path: str) -> tuple[int, str]:
    user_id = int(re.search(r'user_(\d+).*.html', file_path).group(1))
    async with aiofiles.open(file_path, mode='r', encoding='utf-8') as html:
        file_contents = await html.read()
    return user_id, file_contents


def get_user(user_id: int, soup: BeautifulSoup) -> Optional[User]:
    if 'Sign in to learn more about' in soup.find('meta')['content']:
        return None
    elif 'Meet your next favorite book' in soup.find('title').text.strip():
        return None
    elif 'author/show' in soup.find('link')['href']:
        return None

    try:
        user_name = re.search(r'(.+)’s', soup.find('title').text.strip().replace('\n', '')).group(1)
    except AttributeError:
        raise AttributeError(user_id)
    user = User(id=user_id, name=user_name)
    return user


def get_date_published(date_published_str: str) -> Optional[datetime]:
    if date_published_str == 'unknown' or date_published_str.startswith('-'):
        return None
    elif date_published_str.isdigit():
        return None if date_published_str == '0' else datetime(year=int(date_published_str), month=1, day=1)

    date_formats = ['%b %d, %Y', '%b %Y']
    for format_str in date_formats:
        try:
            return datetime.strptime(date_published_str, format_str)
        except ValueError:
            continue
    return None


def get_books_info(soup: BeautifulSoup) -> Optional[list]:
    if soup.find('div', _class='greyText nocontent stacked'):
        return None

    book_info = []

    for book_element in soup.find_all('tr', class_='bookalike'):
        title_element = book_element.find('td', class_='field title').find('a')
        book_url = title_element['href']

        book_url_regex = re.search(r'/book/show/(\d+)', book_url)
        if not book_url_regex:
            raise ValueError(book_url)
        book_id = int(book_url_regex.group(1))

        author_element = book_element.find('td', class_='field author').find('a')

        if not author_element:
            continue

        author_url_regex = re.search(r'/author/show/(\d+)', author_element['href'])
        if not author_url_regex:
            raise ValueError(author_element['href'])
        author_id = int(author_url_regex.group(1))

        isbn = book_element.find('td', class_='field isbn').find('div').text.strip()
        isbn13 = book_element.find('td', class_='field isbn13').find('div').text.strip()
        pages_count_str = book_element.find('td', class_='field num_pages').find('div').text.strip()
        pages_count = None if pages_count_str == 'unknown' else int(re.search(r'(\d+)', pages_count_str).group(1))
        avg_rating = float(book_element.find('td', class_='field avg_rating').find('div').text.strip())
        ratings_count_str = book_element.find('td', class_='field num_ratings').find('div').text
        date_published_str = book_element.find('td', class_='field date_pub').find('div').text.strip()
        user_rating = RATINGS.get(book_element.find('td', class_='field rating').find('span').text.strip(), 0)

        book_info.append({
            'id': book_id,
            'cover': book_element.find('img')['src'],
            'title': title_element['title'],
            'book_url': book_url,
            'author': author_element.text.strip(),
            'author_id': author_id,
            'author_url': author_element['href'],
            'isbn': isbn,
            'isbn13': isbn13,
            'pages_count': pages_count,
            'avg_rating': avg_rating,
            'ratings_count': int(ratings_count_str.strip().replace(',', '')),
            'date_published': get_date_published(date_published_str),
            'user_rating': user_rating
        })

    return book_info


def get_files() -> pd.DataFrame:
    csv_file = r'C:\Home\Work\graduate-work\book-scraper\index.csv'
    command = f'es.exe *.html -path {HTML_DIR} -export-csv {csv_file}'
    subprocess.run(command, check=True)
    df = pd.read_csv(csv_file)
    os.remove(csv_file)
    return df


def filter_files(files_df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    file_paths_to_check = set(files_df['Filename'])
    with sessionmaker(bind=engine)() as session:
        worked_on_files = session.query(File.file_path).all()
        worked_on_file_paths = {row[0] for row in worked_on_files}
        files_to_work_on = file_paths_to_check - worked_on_file_paths
    return pd.DataFrame({'Filename': list(files_to_work_on)})


async def prepare_data_for_db(batch_inserts: set[Record], user_id: int, html: str, pbar: tqdm) -> None:
    soup = BeautifulSoup(html, 'lxml')
    user = get_user(user_id, soup)
    if not user:
        pbar.update(1)
        return
    batch_inserts.add(user)
    try:
        books_info = get_books_info(soup)
    except Exception:
        print(soup)
        raise Exception(user_id)
    if not books_info:
        pbar.update(1)
        return
    for book_info in books_info:
        book = Book(id=book_info['id'], title=book_info['title'], author=book_info['author'],
                    isbn=book_info['isbn'], isbn13=book_info['isbn13'], url=book_info['book_url'],
                    cover_url=book_info['cover'], pages_count=book_info['pages_count'],
                    avg_rating=book_info['avg_rating'], ratings_count=book_info['ratings_count'],
                    date_published=book_info['date_published'])
        rating = Rating(user_id=user_id, book_id=book_info['id'], user_rating=book_info['user_rating'])
        author = Author(id=book_info['author_id'], name=book_info['author'], url=book_info['author_url'])
        batch_inserts.add(book)
        batch_inserts.add(rating)
        batch_inserts.add(author)
    pbar.update(1)


async def get_file_contents(chunk_files: tuple[str]) -> set[tuple[int, str]]:
    tasks = {read_html_file(_file) for _file in chunk_files}
    html_file_contents = set(await asyncio.gather(*tasks))
    return html_file_contents


async def get_inserts(html_file_contents: set[tuple[int, str]], pbar: tqdm) -> set[Record]:
    batch_inserts = set()
    db_tasks = {prepare_data_for_db(batch_inserts, user_id, html, pbar)
                for user_id, html in html_file_contents}
    await asyncio.gather(*db_tasks)
    return batch_inserts


async def main() -> None:
    engine = create_engine(f'sqlite:///{DB_FILE}', echo=False)
    Base.metadata.create_all(engine)

    files_df = get_files()
    print(len(files_df))
    files_df = filter_files(files_df, engine)
    print(len(files_df))

    with tqdm(total=len(files_df), position=0, smoothing=0) as pbar:
        batch_files: tuple[str]
        for batch_files in batched(files_df['Filename'], 2048):
            html_file_contents = await get_file_contents(batch_files)
            batch_inserts = await get_inserts(html_file_contents, pbar)
            batch_inserts = batch_inserts.union({File(file_path=_file) for _file in batch_files})
            with sessionmaker(bind=engine)() as session:
                record: Record
                for record in tqdm(batch_inserts, position=1, leave=False, smoothing=0):
                    session.merge(record)
                session.commit()


if __name__ == '__main__':
    asyncio.run(main())
