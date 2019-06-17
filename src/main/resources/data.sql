insert into genre( id, name) values( 1, 'Проза');
insert into genre( id, name) values( 2, 'Фантастика');
insert into genre( id, name) values( 3, 'Фигастика');

insert into authors( id, name, birthdate, email, phone, address) values(1, 'king', '1980-06-12', 'king@mail.ru', '11111111', 'no info');
insert into authors( id, name, birthdate, email, phone, address) values(2, 'kong', '1990-12-12', 'kong@mail.ru', '22222222', 'address1');
insert into authors( id, name, birthdate, email, phone, address) values(3, 'pong', '2990-12-12', 'pong@mail.ru', '33333333', 'address2');

insert into books( id, name, ISBN, PUB_PLACE, PUB_HOUSE, PUB_YEAR, genre_id, author_id) values( 1, 'book 1', '1111111', 'Moscow', 'Астор',  2018, 1, 1);
insert into books( id, name, ISBN, PUB_PLACE, PUB_HOUSE, PUB_YEAR, genre_id, author_id) values( 2, 'book 2', '2222222', 'SPB',    'Кастор', 2017,2, 1);
insert into books( id,  name,    ISBN,      PUB_PLACE, PUB_HOUSE, PUB_YEAR, genre_id, author_id)
            values( 3, 'book 3', '3333333', 'USA',    'Мастор',   2016,     2,        2);

insert into comments( id, name, datetime, comment, book_id) values( 1, 'test_user1', '2019-01-01 23:59:59', 'Velit aute mollit ipsum ad dolor consectetur nulla officia culpa adipisicing exercitation fugiat tempor. Voluptate deserunt sit sunt nisi aliqua fugiat proident ea ut. Mollit voluptate reprehenderit occaecat nisi ad non minim tempor sunt voluptate consectetur exercitation id ut nulla. Ea et fugiat aliquip nostrud sunt incididunt consectetur culpa aliquip eiusmod dolor. Anim ad Lorem aliqua in cupidatat nisi enim eu nostrud do aliquip veniam minim.', 1);
insert into comments( id, name, datetime, comment, book_id) values( 2, 'test_user2', '2019-04-07 10:01:12', 'Cupidatat quis ad sint excepteur laborum in esse qui. Et excepteur consectetur ex nisi eu do cillum ad laborum. Mollit et eu officia dolore sunt Lorem culpa qui commodo velit ex amet id ex. Officia anim incididunt laboris deserunt anim aute dolor incididunt veniam aute dolore do exercitation. Dolor nisi culpa ex ad irure in elit eu dolore. Ad laboris ipsum reprehenderit irure non commodo enim culpa commodo veniam incididunt veniam ad.', 1);
insert into comments( id, name, datetime, comment, book_id) values( 3, 'test_user3', '2019-05-12 14:56:14', 'Ut ut do pariatur aliquip aliqua aliquip exercitation do nostrud commodo reprehenderit aute ipsum voluptate. Irure Lorem et laboris nostrud amet cupidatat cupidatat anim do ut velit mollit consequat enim tempor. Consectetur est minim nostrud nostrud consectetur irure labore voluptate irure. Ipsum id Lorem sit sint voluptate est pariatur eu ad cupidatat et deserunt culpa sit eiusmod deserunt. Consectetur et fugiat anim do eiusmod aliquip nulla laborum elit adipisicing pariatur cillum.', 1);
