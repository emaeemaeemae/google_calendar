import os
import mysql.connector
import time
import dotenv
import threading

from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from loguru import logger

SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']


def get_mycursor():
    mydb = mysql.connector.connect(
        host='localhost',
        user=os.getenv('db_user'),
        password=os.getenv('db_password'),
        database='calendar_test'
    )

    mycursor = mydb.cursor()
    return mydb, mycursor


def get_creds(company_id):
    creds = None
    if os.path.exists(f'token_{company_id}.json'):
        creds = Credentials.from_authorized_user_file(f'token_{company_id}.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(f'token_{company_id}.json', 'w') as token:
            token.write(creds.to_json())
    return creds


def get_companies(mycursor):
    sql_select = f'SELECT id from company'
    mycursor.execute(sql_select)
    result = [x[0] for x in mycursor.fetchall()]
    return result


def get_halls(mycursor, company_id):
    sql_select = f'SELECT id, google_calendar_id from hall where company_id = {company_id}'
    mycursor.execute(sql_select)
    result = mycursor.fetchall()
    return result


def get_events(service, hall, first_start=False):
    now = (datetime.utcnow() - timedelta(days=50000 if first_start else 0)).isoformat() + 'Z'
    events_result = service.events().list(calendarId=hall, timeMin=now,
                                          maxResults=10000, singleEvents=True,
                                          orderBy='startTime').execute()
    return events_result.get('items', [])


def check_crossing_events(hall_id, date_start, date_end, time_zone, google_id, mycursor):
    """
        Проверка на пересечение с текущими событиями
        в случае пересечения возвращает список id событий
    """

    def check_date_crossing(date1, date2):
        t1start = datetime.strptime(f'{date1[0]} +{date1[2]}', '%Y-%m-%dT%H:%M:%S %z')
        t1end = datetime.strptime(f'{date1[1]} +{date1[2]}', '%Y-%m-%dT%H:%M:%S %z')
        t2start = datetime.strptime(f'{date2[0]} +{date2[2]}', '%Y-%m-%dT%H:%M:%S %z')
        t2end = datetime.strptime(f'{date2[1]} +{date2[2]}', '%Y-%m-%dT%H:%M:%S %z')
        return (t1start <= t2start <= t1end) or (t2start <= t1start <= t2end)

    sql_select = f'SELECT id, DATE_FORMAT(date_start, "%Y-%m-%dT%H:%i:%s"), DATE_FORMAT(date_end, "%Y-%m-%dT%H:%i:%s"), time_zone, google_id ' \
                 f'from event where hall_id = {hall_id}'
    mycursor.execute(sql_select)
    events = mycursor.fetchall()
    result = set()
    for event in events:
        if google_id != event[4] and check_date_crossing((date_start, date_end, time_zone), (event[1], event[2], event[3])):
            result.add(event[0])
    return result if result else False


def insert_event(mydb, mycursor, insert_tuple):
    sql_insert = f'insert into event (company_id, hall_id, google_id, date_start, date_end, time_zone, error) ' \
                 f'values (%s, %s, %s, %s, %s, %s, %s) ' \
                 f'on duplicate key update hall_id = "{insert_tuple[1]}",' \
                 f'date_start = "{insert_tuple[3]}", date_end = "{insert_tuple[4]}", time_zone = "{insert_tuple[5]}", ' \
                 f'error = {insert_tuple[6]}'
    mycursor.execute(sql_insert, insert_tuple)
    mydb.commit()
    logger.info(f'Обновлена/Добавлена запись компании №{insert_tuple[0]} Зал №{insert_tuple[1]} '
                f'Дата начала: {insert_tuple[3]} Дата окончания: {insert_tuple[4]}')


def update_crossing(mydb, mycursor, lst):
    s = 'id = ' + ' or id = '.join(list(map(str, lst)))
    sql_update = f'update event set error = 1 where {s}'
    mycursor.execute(sql_update)
    mydb.commit()


def update_company(mydb, mycursor, company_id, first_start=False):
    halls = get_halls(mycursor, company_id)
    creds = get_creds(company_id)
    service = build('calendar', 'v3', credentials=creds)

    for hall in halls:
        events = get_events(service, hall[1], first_start)
        for event in events[::-1]:
            google_id = event.get('id')
            start = event.get('start').get('dateTime')[:19]
            end = event.get('end').get('dateTime')[:19]
            time_zone = event.get('start').get('dateTime')[20:25].replace(':', '')
            crossing_list = check_crossing_events(hall[0], start, end, time_zone, google_id, mycursor)
            if crossing_list:
                error = 1
                update_crossing(mydb, mycursor, crossing_list)
            else:
                error = 0
            insert_event(mydb, mycursor, (company_id, hall[0], google_id, start, end, time_zone, error))


@logger.catch
def main(offset):
    mydb, mycursor = get_mycursor()

    while True:
        companies_lst = get_companies(mycursor)

        logger.info('Начинаю новый цикл обновления блока компаний')
        for i in range(offset, len(companies_lst), int(os.getenv('parallel_tasks'))):
            update_company(mydb, mycursor, companies_lst[i])
        time.sleep(int(os.getenv('timeout')))
        logger.info('Компании успешно обновлены')


if __name__ == '__main__':
    dotenv_file = dotenv.find_dotenv()
    dotenv.load_dotenv(dotenv_file)
    logger.add('update_db.log', format='{time} {level} {message}', level='DEBUG', rotation='3 MB', compression='zip')

    threads_list = []
    for i in range(int(os.getenv('parallel_tasks'))):
        threads_list.append(threading.Thread(target=main, args=[i]))

    for thread in threads_list:
        thread.start()
