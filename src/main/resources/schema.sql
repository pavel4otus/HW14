DROP TABLE IF EXISTS authors;
CREATE TABLE authors(
  ID IDENTITY NOT NULL PRIMARY KEY,
  NAME VARCHAR(255),
  BIRTHDATE DATE,
  EMAIL VARCHAR(50),
  PHONE VARCHAR(20),
  ADDRESS VARCHAR(255));

DROP TABLE IF EXISTS genre;
CREATE TABLE genre( ID IDENTITY NOT NULL PRIMARY KEY, NAME varChar2(250));


DROP TABLE IF EXISTS books;
create table books(
  ID   IDENTITY NOT NULL PRIMARY KEY,
  NAME varchar(250),
  ISBN varchar(250),
  PUB_PLACE varchar(250),
  PUB_YEAR  NUMBER,
  PUB_HOUSE  varchar(250),
  genre_id     LONG,
  author_id    LONG
);
alter table books add foreign key( genre_id)  references genre(id);
alter table books add foreign key( author_id) references authors(id);


DROP TABLE IF exists comments;
create table comments(
  ID   IDENTITY NOT NULL PRIMARY KEY,
  NAME varchar(250),
  datetime DATETIME,
  comment varchar(4000),
  book_id LONG
);
alter table comments add foreign key( book_id) references books(id);