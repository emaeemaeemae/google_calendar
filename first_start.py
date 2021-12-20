import dotenv

from update_db import get_mycursor, update_company
from loguru import logger


@logger.catch
def main():
    def insert_users(user_name):
        sql_insert = f'insert into company (name) values ("{user_name}")'
        mycursor.execute(sql_insert)
        mydb.commit()

    def insert_hall(insert_list):
        insert_tuple = tuple([int(insert_list[0]), insert_list[1], insert_list[2]])
        sql_insert = f'insert into hall (company_id, name, google_calendar_id) values (%s, %s, %s)'
        mycursor.execute(sql_insert, insert_tuple)
        mydb.commit()

    logger.add('first_start.log', format='{time} {level} {message}', level='DEBUG', rotation='3 MB', compression='zip')

    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file)

    # Ввод юзеров
    mydb, mycursor = get_mycursor()
    insert_users(input('Введите имя первого юзера: '))
    insert_users(input('Введите имя второго юзера: '))


    # Ввод календарей (google_calendar_id)
    print('Имя и id вводить через пробел')
    # insert_hall([1] + input('Введите имя и google_calendar_id первого календаря (юзер 1): ').split())
    insert_hall([1] + input('Введите имя и google_calendar_id первого календаря (юзер 2): ').split())
    insert_hall([1] + input('Введите имя и google_calendar_id второго календаря (юзер 2): ').split())
    insert_hall([2] + input('Введите имя и google_calendar_id третьего календаря (юзер 2): ').split())
    # insert_hall([2] + input('Введите имя и google_calendar_id четвертого календаря (юзер 2): ').split())
    logger.info(f'Add Users and Halls')

    update_company(mydb, mycursor, 1, first_start=True)
    update_company(mydb, mycursor, 2, first_start=True)


if __name__ == '__main__':
    main()
