import mysql.connector
import os
import dotenv

from loguru import logger


def db_create():
    mydb = mysql.connector.connect(
        host='localhost',
        user=os.getenv('db_user'),
        password=os.getenv('db_password')
    )

    logger.info(f'Connected to Database')

    mycursor = mydb.cursor()

    mycursor.execute(f'CREATE DATABASE IF NOT EXISTS calendar_test')

    logger.info(f'Database successfully created')

    mydb = mysql.connector.connect(
        host='localhost',
        user=os.getenv('db_user'),
        password=os.getenv('db_password'),
        database='calendar_test'
    )

    mycursor = mydb.cursor()

    logger.info(f'Connected to Database "calendar_test"')

    sql_create_table = '''CREATE TABLE IF NOT EXISTS company(
           id INT(11) AUTO_INCREMENT PRIMARY KEY,
           name VARCHAR(200) NOT NULL)'''

    mycursor.execute(sql_create_table)

    logger.info(f'Table company successfully created')

    sql_create_table = '''CREATE TABLE IF NOT EXISTS hall(
           id INT(11) AUTO_INCREMENT PRIMARY KEY,
           company_id INT(11),
           FOREIGN KEY (company_id) REFERENCES company (id) ON DELETE CASCADE,
           name VARCHAR(200) NOT NULL,
           google_calendar_id VARCHAR(200) NOT NULL UNIQUE)'''

    mycursor.execute(sql_create_table)

    logger.info(f'Table hall successfully created')

    sql_create_table = '''CREATE TABLE IF NOT EXISTS event(
           id INT(11) AUTO_INCREMENT PRIMARY KEY,
           company_id INT(11),
           FOREIGN KEY (company_id) REFERENCES company (id) ON DELETE CASCADE,
           hall_id INT(11),
           FOREIGN KEY (hall_id) REFERENCES hall (id) ON DELETE CASCADE,
           google_id VARCHAR(255) UNIQUE,
           date_start DATETIME NOT NULL,
           date_end DATETIME NOT NULL,
           time_zone VARCHAR(4) NOT NULL DEFAULT '0', 
           error TINYINT(1) NOT NULL DEFAULT 0)'''

    mycursor.execute(sql_create_table)

    logger.info(f'Table event successfully created')


@logger.catch
def main():
    logger.add('db_create.log', format='{time} {level} {message}', level='DEBUG', rotation='3 MB', compression='zip')
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file)
    db_create()


if __name__ == '__main__':
    main()
