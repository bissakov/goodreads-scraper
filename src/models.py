from sqlalchemy import Column, Date, Float, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class User(Base):
    __tablename__ = 'User'

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(50), nullable=False)

    def __eq__(self, other):
        if isinstance(other, User):
            return self.id == other.id
        return False

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):
        return f'<User(id={self.id}, name={self.name})>'


class Rating(Base):
    __tablename__ = 'Rating'

    id = Column(Integer, autoincrement=True, primary_key=True, nullable=False)
    user_id = Column(Integer, ForeignKey('User.id'), nullable=False)
    book_id = Column(Integer, ForeignKey('Book.id'), nullable=False)
    user_rating = Column(Integer, nullable=False)

    User = relationship('User')
    Book = relationship('Book')

    def __eq__(self, other):
        if isinstance(other, Rating):
            return self.user_id == other.user_id and self.book_id == other.book_id
        return False

    def __hash__(self):
        return hash((self.user_id, self.book_id))

    def __repr__(self):
        return (f'<Rating(id={self.id}, user_id={self.user_id},'
                f'book_id={self.book_id}, user_rating={self.user_rating})>')


class Author(Base):
    __tablename__ = 'Author'

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(50), nullable=False)
    url = Column(String(100), nullable=False)

    def __eq__(self, other):
        if isinstance(other, Author):
            return self.id == other.id
        return False

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):
        return f'<Author(id={self.id}, name={self.name})>'


class Book(Base):
    __tablename__ = 'Book'

    id = Column(Integer, primary_key=True, nullable=False)
    title = Column(String(100), nullable=False)
    author = Column(String(50), nullable=False)
    isbn = Column(String(13), nullable=False)
    isbn13 = Column(String(13), nullable=False)
    url = Column(String(100), nullable=False)
    cover_url = Column(String(100), nullable=False)
    pages_count = Column(Integer, nullable=True)
    avg_rating = Column(Float, nullable=False)
    ratings_count = Column(Integer, nullable=False)
    date_published = Column(Date, nullable=True)

    def __eq__(self, other):
        if isinstance(other, Book):
            return self.id == other.id
        return False

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):
        return (f'<Book(id={self.id}, title={self.title}, author={self.author},'
                f'isbn={self.isbn}, isbn13={self.isbn13}, url={self.url},'
                f'cover_url={self.cover_url}, pages_count={self.pages_count},'
                f'avg_rating={self.avg_rating}, ratings_count={self.ratings_count},'
                f'date_published={self.date_published})>')
