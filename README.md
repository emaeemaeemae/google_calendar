1. В файле .env указать логин/пароль для локальной БД, таймаут обновления, число одновременно обновляемых компаний.
2. Запустить файл db_create.py для создания таблиц
3. Запустить файл first_start.py
   1. Указать имя первого юзера (компании)
   2. Указать имя второго юзера (компании)
   3. Указать имена залов и их id через пробел (см. подсказки)
4. Запустить update_db.py
   
При попытке загрузить события с не авторизированного юзера всплывет окно гугл регистрации, выбрать в нем нужного пользователя.
Как запросить данные по чистому токену не нашел, поэтому убрал эту колонку и добавил авторизацию с последующим созданием json файла для пользователя.