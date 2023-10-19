import argparse
import asyncio
import os
import re
import subprocess
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from itertools import batched
from os.path import abspath, dirname, join
from typing import Optional, Union

import aiofiles
import aiofiles.os
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from src.models import Author, Book, Rating, User

Record = Union[Author, Book, Rating, User]

RATINGS = {
    'it was amazing': 5,
    'really liked it': 4,
    'liked it': 3,
    'it was ok': 2,
    'did not like it': 1
}


@dataclass(frozen=True)
class BookInfo:
    id: int
    cover_url: str
    title: str
    book_url: str
    author: str
    author_id: int
    author_url: str
    isbn: str
    isbn13: str
    pages_count: Optional[int]
    avg_rating: float
    ratings_count: int
    date_published: Optional[datetime]
    user_rating: int


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Goodreads HTML Scraper')
    parser.add_argument('--folder', type=str,
                        help='Folder to save HTML files',
                        required=True, default=r'C:\Home\html')
    parser.add_argument('--chunk_size', type=int,
                        help='Chunk size', default=2048)
    return parser.parse_args()


def get_user_id_from_path(file_path: str) -> int:
    user_id_pattern = re.compile(r'user_(\d+).*.html')
    match = user_id_pattern.search(file_path)
    if match:
        return int(match.group(1))
    raise ValueError(f'Invalid file path format: {file_path}')


async def read_html_file(file_path: str) -> tuple[int, str]:
    user_id = get_user_id_from_path(file_path)
    async with aiofiles.open(file_path, mode='r', encoding='utf-8') as html:
        file_contents = await html.read()
    return user_id, file_contents


def extract_user(user_id: int, soup: BeautifulSoup) -> Optional[User]:
    black_list_terms = {'Sign in to learn more', 'author/show', 'Meet your next favorite book'}
    content = soup.find('meta')['content']
    if any(term in content for term in black_list_terms):
        return None

    user_name_match = re.search(r'(.+)’s', soup.find('title').text.strip())
    if user_name_match:
        return User(id=user_id, name=user_name_match.group(1))
    return None


def get_date_published(date_str: str) -> Optional[datetime]:
    if date_str in {'unknown', '0'} or date_str.startswith('-'):
        return None

    date_formats = ['%b %d, %Y', '%b %Y', '%Y', '%y']
    for format_str in date_formats:
        with suppress(ValueError):
            return datetime.strptime(date_str, format_str)
    return None


def extract_books(soup: BeautifulSoup) -> Optional[set[BookInfo]]:
    if soup.find('div', class_='greyText nocontent stacked'):
        return None

    books_info = set()
    for book_element in soup.find_all('tr', class_='bookalike'):
        title_element = book_element.find('td', class_='field title').find('a')
        book_url = title_element['href']

        book_id_match = re.search(r'/book/show/(\d+)', book_url)
        if not book_id_match:
            raise ValueError(book_url)
        book_id = int(book_id_match.group(1))

        try:
            author_element = book_element.find('td', class_='field author').find('a')
            author_id_match = re.search(r'/author/show/(\d+)', author_element['href'])
            if not author_id_match:
                continue
            author_id = int(author_id_match.group(1))
        except TypeError:
            continue

        isbn = book_element.find('td', class_='field isbn').find('div').text.strip()
        isbn13 = book_element.find('td', class_='field isbn13').find('div').text.strip()
        pages_count_str = book_element.find('td', class_='field num_pages').find('div').text.strip()
        pages_count_match = re.search(r'(\d+)', pages_count_str)
        pages_count = None if pages_count_str == 'unknown' else int(pages_count_match.group(1))
        avg_rating = float(book_element.find('td', class_='field avg_rating').find('div').text.strip())
        ratings_count_str = book_element.find('td', class_='field num_ratings').find('div').text
        ratings_count = int(ratings_count_str.replace(',', '').strip())
        date_published_str = book_element.find('td', class_='field date_pub').find('div').text.strip()
        user_rating_text = book_element.find('td', class_='field rating').find('span').text.strip()
        user_rating = RATINGS.get(user_rating_text, 0)

        books_info.add(BookInfo(
            id=book_id,
            cover_url=book_element.find('img')['src'],
            title=title_element['title'],
            book_url=book_url,
            author=author_element.text.strip(),
            author_id=author_id,
            author_url=author_element['href'],
            isbn=isbn,
            isbn13=isbn13,
            pages_count=pages_count,
            avg_rating=avg_rating,
            ratings_count=ratings_count,
            date_published=get_date_published(date_published_str),
            user_rating=user_rating
        ))
    return books_info


def extract_file_list(html_dir: str, csv_tmp_file: str) -> pd.DataFrame:
    command = f'es.exe *.html -path {html_dir} -export-csv {csv_tmp_file}'
    subprocess.run(command, check=True)
    df = pd.read_csv(csv_tmp_file)
    os.remove(csv_tmp_file)
    return df


async def prepare_data_for_db(batch_inserts: set[Record], user_id: int, html: str, pbar: tqdm) -> None:
    soup = BeautifulSoup(html, 'lxml')
    user = extract_user(user_id, soup)
    if not user:
        pbar.update(1)
        return None

    batch_inserts.add(user)
    books_info = extract_books(soup)

    if not books_info:
        pbar.update(1)
        return None

    for book_info in books_info:
        book = Book(id=book_info.id, title=book_info.title, author=book_info.author,
                    isbn=book_info.isbn, isbn13=book_info.isbn13, url=book_info.book_url,
                    cover_url=book_info.cover_url, pages_count=book_info.pages_count,
                    avg_rating=book_info.avg_rating, ratings_count=book_info.ratings_count,
                    date_published=book_info.date_published)
        rating = Rating(user_id=user_id, book_id=book_info.id, user_rating=book_info.user_rating)
        author = Author(id=book_info.author_id, name=book_info.author, url=book_info.author_url)
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


async def remove_files(batch_files: tuple[str]):
    tasks = {aiofiles.os.remove(file_path) for file_path in batch_files}
    await asyncio.gather(*tasks)


def merge_records_to_db(batch_inserts: set[Record], engine: Engine):
    pbar = tqdm(total=len(batch_inserts), position=1, leave=False)
    session = sessionmaker(bind=engine)()
    with session, pbar:
        for model_cls in [User, Author, Book, Rating]:
            for record in filter(lambda r: isinstance(r, model_cls), batch_inserts):
                session.merge(record)
                pbar.update(1)
        session.commit()


async def main() -> None:
    args = get_args()
    html_dir = args.folder
    chunk_size = args.chunk_size
    project_folder = dirname(dirname(abspath(__file__)))
    csv_tmp_file = join(project_folder, 'tmp.csv')
    db_file = join(project_folder, 'goodreads.db')

    files_df = extract_file_list(html_dir, csv_tmp_file)

    engine = create_engine(f'sqlite:///{db_file}', echo=False)
    with tqdm(total=len(files_df), position=0) as pbar:
        batch_files: tuple[str]
        for batch_files in batched(files_df['Filename'], chunk_size):
            html_file_contents = await get_file_contents(batch_files)
            batch_inserts = await get_inserts(html_file_contents, pbar)

            await remove_files(batch_files)
            merge_records_to_db(batch_inserts, engine)


if __name__ == '__main__':
    asyncio.run(main())
