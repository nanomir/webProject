import sqlite3 as sql
import pandas as pd
connection = sql.connect("store.sqlite")

fileDamp = open('store.db','r', encoding ='utf-8-sig')
damp = fileDamp.read()
fileDamp.close()
connection.executescript(damp)
connection.commit()
cursor = connection.cursor()
cursor.execute('''
SELECT 
   buy.buy_id as заказ,
   city.name_city as город,
   book.title as книга,
   buy_book.amount as количество
FROM 
   buy, buy_step, client, city, book, buy_book
WHERE 
   buy.buy_id = buy_step.buy_id AND 
   date_step_end IS NOT NULL AND
   buy_step.step_id = 4 AND
   date_step_end <= DATE('now') AND
   buy.client_id = client.client_id AND
   city.city_id = client.city_id AND
   buy.buy_id = buy_book.buy_id AND
   buy_book.book_id = book.book_id
ORDER BY  
   buy.buy_id, book.title ASC
''')
df = pd.read_sql('''
SELECT 
   buy.buy_id as заказ,
   city.name_city as город,
   book.title as книга,
   buy_book.amount as количество
FROM 
   buy, buy_step, client, city, book, buy_book
WHERE 
   buy.buy_id = buy_step.buy_id AND 
   date_step_end IS NOT NULL AND
   buy_step.step_id = 4 AND
   date_step_end <= DATE('now') AND
   buy.client_id = client.client_id AND
   city.city_id = client.city_id AND
   buy.buy_id = buy_book.buy_id AND
   buy_book.book_id = book.book_id
ORDER BY  
   buy.buy_id, book.title ASC
''', connection)
print (df)

df = pd.read_sql('''
SELECT
   buy.buy_id as заказ,
   SUBSTR(client.name_client, 1, INSTR(client.name_client, ' ')) as клиент,
   SUM(book.price * buy_book.amount) as стоимость
FROM client, buy, buy_book, book
WHERE
   client.client_id = buy.client_id AND
   buy.buy_id = buy_book.buy_id AND
   buy_book.book_id = book.book_id
GROUP BY
   client.name_client
HAVING 
   стоимость NOT BETWEEN 500 AND 2000
ORDER BY
   стоимость DESC, клиент ASC       
''', connection)
print (df)
df = pd.read_sql('''
SELECT
      book.title as название,
      genre.name_genre as жанр,
      SUM(buy_book.amount) as количество
FROM
   book, genre, buy_book
WHERE
   book.genre_id = genre.genre_id AND
   buy_book.book_id = book.book_id 
GROUP BY
      book.title
''',connection)
print (df)


df = pd.read_sql('''
with genre_amount as(
select 
      genre.genre_id as g,
      SUM(buy_book.amount) as buyAmount

from buy_book, book, genre
WHERE
   genre.genre_id = book.genre_id AND
   buy_book.book_id = book.book_id
GROUP BY
   g
)
select book.title as название from genre_amount, book
WHERE book.genre_id = g AND
buyAmount=(select MAX(buyAmount) from genre_amount)             
''',connection)
print (df)

print(pd.read_sql('''
SELECT * FROM book;
''', connection))
cursor.execute('''
WITH AvgSold AS (
    SELECT 
        buy_book.book_id,
        COUNT(buy_book.buy_book_id) AS total_sold,
        AVG(COUNT(buy_book.buy_book_id)) OVER () AS avg_sold
    FROM 
        buy_book 
    GROUP BY 
        buy_book.book_id
)

UPDATE book
SET price = 
    CASE 
        WHEN bookWithTotal.total_sold > avg.avg_sold THEN
            ROUND(price * 1.1, 2)
        ELSE
            ROUND(price * 0.95, 2)
    END
FROM  (
    SELECT buy_book.book_id, COUNT(buy_book.buy_book_id) AS total_sold
    FROM buy_book buy_book
    GROUP BY buy_book.book_id
) bookWithTotal
               
JOIN AvgSold avg ON bookWithTotal.book_id = avg.book_id;
''')
print(pd.read_sql('''
SELECT * FROM book;
''', connection))

df = pd.read_sql('''
WITH RankedBooks AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY amount DESC) AS "Nпп",
        CASE 
            WHEN LENGTH(title)>15 THEN
                  SUBSTR(title, 1, 12) || '...'
                ELSE
                  title
        END AS "Книга",
        name_author AS "Автор",
        amount AS "Кол-во",
        RANK() OVER (ORDER BY amount DESC) AS "Ранг",
        PERCENT_RANK() OVER (ORDER BY amount DESC)*100 AS "Ранг,%",
        SUM(amount) OVER (ORDER BY amount DESC) AS "Распределение"
    FROM book
    JOIN author ON book.author_id = author.author_id
)
SELECT
    "Nпп",
    "Автор",
    "Книга",
    "Кол-во",
    "Ранг",
    "Распределение",
    ROUND("Ранг,%", 2) AS "Ранг,%"
FROM RankedBooks
ORDER BY "Кол-во" DESC;
''', connection)
print(df)

#print(cursor.fetchall())

