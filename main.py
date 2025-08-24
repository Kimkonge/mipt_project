import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
import json
import psycopg2
import os
import shutil
import glob

#Настройка папок

input_folder = "data"
archive_folder = "archive"
os.makedirs(archive_folder, exist_ok=True)
#Подключение к серверу 
with open("cred.json", "r") as f:
    cred = json.load(f)

url = f"postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}"    
engine = create_engine(url)
conn = psycopg2.connect(**cred)
cursor = conn.cursor()


#Создаем функцию загрузки транзакций за день во временную таблицу stg_transaction_auto(все данные VARCHAR)
def transaction2sql(
    path: str,
    fixed_length: int = 255
):
    df = pd.read_csv(
        path,
        sep=';',           # разделитель - точка с запятой
        decimal=','       # десятичный разделитель - запятая
    )
    dtype_mapping = {col: VARCHAR(length=fixed_length) for col in df.columns}   
    df.to_sql(
        name="stg_transaction_auto",
        con=engine,
        schema="project",
        if_exists="replace",
        index=False,
        dtype=dtype_mapping
    )


#Создаем функцию загрузки терминалов за день во временную таблицу stg_terminal_auto
def terminal2sql(path):
    df = pd.read_excel(path)
    df.to_sql(
        name="stg_terminal_auto", 
        con=engine, 
        schema="project", 
        if_exists="replace", 
        index=False,
    )
#Создаем функцию загрузки черных паспортов за день во временную таблицу stg_black_passport_auto
def passport2sql(path):
    df = pd.read_excel(path)
    df.to_sql(
        name="stg_black_passport_auto", 
        con=engine, 
        schema="project", 
        if_exists="replace", 
        index=False
    )


# Функция, которая создает таблицу фактов с транзакциями.
def create_dwh_fact_transaction():
    cursor.execute(
        """
        create table if not exists project.dwh_fact_transaction(
            trans_id varchar(255),
            trans_date timestamp,
            amt numeric(12,2),
            card_num varchar(255),
            oper_type varchar(255),
            oper_result varchar(255),
            terminal_id varchar(255)         
        )
        """
    )
    conn.commit()

# Функция, которая добавляет в таблицу фактов с транзакциями новые записи.
def add_dwh_fact_transaction():
    cursor.execute(
        """ 
        INSERT INTO project.dwh_fact_transaction(
            trans_id,
            trans_date,
            amt,
            card_num,
            oper_type,
            oper_result,
            terminal_id
        )
        SELECT 
            transaction_id,
            TO_TIMESTAMP(transaction_date, 'YYYY-MM-DD HH24:MI:SS'),      
            CAST(REPLACE(amount, ',', '.') AS NUMERIC(12,2)),
            card_num,
            oper_type,
            oper_result,
            terminal 
        FROM project.stg_transaction_auto stg
        WHERE NOT EXISTS (
            SELECT 1
            FROM project.dwh_fact_transaction dwh
            WHERE dwh.trans_id = stg.transaction_id
        )
    """
    )
    conn.commit()

# Функция, которая создает таблицу с паспортами из "черного списка"

def create_dwh_fact_passport_blacklist():
    cursor.execute(
        """
        create table if not exists project.dwh_fact_passport_blacklist(
            passport_num varchar(255),
            entry_dt date        
        )
        """
    )
    conn.commit() 

# Функция, которая добавляет в таблицу с паспортами из "черного списка" новые паспорта

def add_dwh_fact_passport_blacklist():
    cursor.execute(
        """ 
        INSERT INTO project.dwh_fact_passport_blacklist(
            passport_num,
            entry_dt
        )
        SELECT 
            passport,
            date         
        FROM project.stg_black_passport_auto stg
        WHERE NOT EXISTS (
            SELECT 1
            FROM project.dwh_fact_passport_blacklist dwh
            WHERE dwh.passport_num = stg.passport
        )
    """
    )
    conn.commit()

# Функция, которая создает таблицу с терминалами в SCD2 формате.

def create_dwh_dim_terminal_hist():
    cursor.execute(
        """
        create table if not exists project.dwh_dim_terminal_hist(
            terminal_id varchar(255),
            terminal_type varchar(255),
            terminal_city varchar(255),
            terminal_address varchar(255),
            deleted_flg integer default 0,
            start_dttm timestamp default current_timestamp,
            end_dttm timestamp default ('5999-12-31 23:59:59'::timestamp)
        )
        """
    )
    conn.commit()


#  функция, которая создает представление с актуальным срезом терминалов

def create_v_terminal():
    cursor.execute("drop view if exists project.v_terminal")
    cursor.execute(
        """
        create view project.v_terminal as
            select
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address    
            from project.dwh_dim_terminal_hist
            where deleted_flg = 0 
            and current_timestamp between start_dttm and end_dttm   
        """
    )
    conn.commit()

# Функция, которая обрабатывает новые записи с терминалами.

def insert_new_terminals():
    cursor.execute(
        """
        insert into project.dwh_dim_terminal_hist (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            deleted_flg,
            start_dttm,
            end_dttm
        )
        select 
            stg.terminal_id,
            stg.terminal_type,
            stg.terminal_city,
            stg.terminal_address,
            0,
            current_timestamp,
            '5999-12-31 23:59:59'::timestamp
        from project.stg_terminal_auto stg
        left join project.dwh_dim_terminal_hist dwh
            on stg.terminal_id = dwh.terminal_id
        where dwh.terminal_id is null
        """
    )
    conn.commit()

# Функция, которая обновляет старые записи с терминалами.
def update_changed_terminals():
    # Закрываем старую запись
    cursor.execute(
        """
        update project.dwh_dim_terminal_hist dwh
        set end_dttm = current_timestamp - interval '1 second'
        where dwh.end_dttm = '5999-12-31 23:59:59'
        and exists (
            select 1
            from project.stg_terminal_auto stg
            where stg.terminal_id = dwh.terminal_id
              and (
                  stg.terminal_type   is distinct from dwh.terminal_type
               or stg.terminal_city   is distinct from dwh.terminal_city
               or stg.terminal_address is distinct from dwh.terminal_address
              )
        )
        """
    )

    # Вставляем новую версию
    cursor.execute(
        """
        insert into project.dwh_dim_terminal_hist (
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            deleted_flg,
            start_dttm,
            end_dttm
        )
        select 
            stg.terminal_id,
            stg.terminal_type,
            stg.terminal_city,
            stg.terminal_address,
            0,
            current_timestamp,
            '5999-12-31 23:59:59'::timestamp
        from project.stg_terminal_auto stg
        join project.dwh_dim_terminal_hist dwh
            on stg.terminal_id = dwh.terminal_id
        where dwh.end_dttm = current_timestamp - interval '1 second'  
        """
    )
    conn.commit()

# Функция, которая устанавливает флаг=1 у удаленных терминалов

def mark_deleted_terminals():
    cursor.execute(
        """
        update project.dwh_dim_terminal_hist dwh
        set end_dttm = current_timestamp - interval '1 second',
            deleted_flg = 1
        where dwh.end_dttm = '5999-12-31 23:59:59'
          and not exists (
              select 1
              from project.stg_terminal_auto stg
              where stg.terminal_id is not distinct from dwh.terminal_id
          )
        """
    )
    conn.commit()

# Функция, которая удаляет stg таблицы(запускать по необходимости)
def drop_stg_tables():
    cursor.execute("DROP TABLE IF EXISTS project.stg_terminal_auto")
    cursor.execute("DROP TABLE IF EXISTS project.stg_transaction_auto")
    cursor.execute("DROP TABLE IF EXISTS project.stg_black_passport_auto")
    conn.commit()

# Функция, которая удаляет dwh и rep таблицы(запускать по необходимости)
def drop_dwh_and_rep_tables():
    cursor.execute("DROP TABLE IF EXISTS project.dwh_fact_transaction")
    cursor.execute("DROP TABLE IF EXISTS project.dwh_fact_passport_blacklist")
    cursor.execute("DROP TABLE IF EXISTS project.dwh_dim_terminal_hist CASCADE")
    cursor.execute("DROP TABLE IF EXISTS project.rep_fraud")
    conn.commit()

# Функция, которая создает таблицу rep_fraud

def create_rep_fraud():
    cursor.execute(
        """
        create table if not exists project.rep_fraud(
            event_dt timestamp,
            passport varchar(255),
            fio varchar(255),
            phone varchar(255),
            event_type varchar(255),
            report_dt timestamp DEFAULT current_timestamp
        )
        """
    )
    conn.commit()

# Функция, заггружает в отчет мошеннические операции по признаку просроченного паспорта или паспорта в черном списке

def load_rep_fraud1():
    cursor.execute(
        """
        insert into project.rep_fraud(event_dt, passport, fio, phone, event_type)
            select
                t.trans_date as event_dt,
                c.passport_num as passport,
                c.last_name || ' ' || c.first_name || ' ' || c.patronymic as fio,
                c.phone,
                'Просроченный паспорт' as event_type
            from project.dwh_fact_transaction t
            inner join project.cards cr
            on t.card_num = cr.card_num
            inner join project.accounts a
            on cr.account = a.account
            inner join project.clients c
            on a.client = c.client_id
            where c.passport_valid_to IS NOT NULL
            and c.passport_valid_to < t.trans_date
            and not exists (
                select 1
                from project.rep_fraud r
                where r.passport = c.passport_num
                    and r.event_dt = t.trans_date::timestamp
                    and r.event_type = 'Просроченный паспорт'
            )
            union all
            select
                t.trans_date as event_dt,
                c.passport_num as passport,
                c.last_name || ' ' || c.first_name || ' ' || c.patronymic as fio,
                c.phone,
                'Заблокированный паспорт' as event_type
            from project.dwh_fact_transaction t
            inner join project.cards cr
            on t.card_num = cr.card_num
            inner join project.accounts a
            on cr.account = a.account
            inner join project.clients c
            on a.client = c.client_id
            inner join project.dwh_fact_passport_blacklist bl
            on c.passport_num = bl.passport_num
            and bl.entry_dt <= t.trans_date
             and not exists (
                    select 1
                    from project.rep_fraud r
                    where r.passport = c.passport_num
                        and r.event_dt = t.trans_date::timestamp
                        and r.event_type = 'Заблокированный паспорт'
                )
        """
    )
    conn.commit()

# Функция, заггружает в отчет мошеннические операции по признаку недействительного договора

def load_rep_fraud2():
    cursor.execute(
        """
            INSERT INTO project.rep_fraud(
                event_dt,
                passport,
                fio,
                phone,
                event_type
            )
            SELECT
                t.trans_date AS event_dt,
                c.passport_num AS passport,
                CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic) AS fio,
                c.phone,
                'Операция по недействующему договору' AS event_type
            FROM project.dwh_fact_transaction t
            JOIN project.cards cr
                ON t.card_num = cr.card_num
            JOIN project.accounts a
                ON cr.account = a.account
            JOIN project.clients c
                ON a.client = c.client_id
            WHERE t.trans_date > a.valid_to
            AND NOT EXISTS (
                SELECT 1
                FROM project.rep_fraud r
                WHERE r.event_dt = t.trans_date
                    AND r.passport = c.passport_num
                    AND r.event_type = 'Операция по недействующему договору'
  )
        """
    )
    conn.commit()

# Функция, заггружает в отчет мошеннические операции по признаку совершения операций в разных городах в течение одного часа.

def load_rep_fraud3():
    cursor.execute(
        """
        INSERT INTO project.rep_fraud(
            event_dt,
            passport,
            fio,
            phone,
            event_type
        )
        WITH client_tx AS (
            SELECT 
                t.trans_id,
                t.trans_date,
                cr.account,
                a.client,
                t.card_num,
                t.terminal_id,
                th.terminal_city
            FROM project.dwh_fact_transaction t
            JOIN project.cards cr ON t.card_num = cr.card_num
            JOIN project.accounts a ON cr.account = a.account
            JOIN project.dwh_dim_terminal_hist th ON t.terminal_id = th.terminal_id
        ),
        suspicious AS (
            SELECT DISTINCT t1.trans_id, t1.client, t1.trans_date AS event_dt
            FROM client_tx t1
            JOIN client_tx t2
              ON t1.client = t2.client
             AND t1.trans_id <> t2.trans_id
             AND t1.terminal_city <> t2.terminal_city
             AND ABS(EXTRACT(EPOCH FROM (t1.trans_date - t2.trans_date))) <= 3600
        )
        SELECT
            s.event_dt,
            cl.passport_num AS passport,
            CONCAT(cl.last_name, ' ', cl.first_name, ' ', cl.patronymic) AS fio,
            cl.phone,
            'Операции в разных городах в течение часа' AS event_type
        FROM suspicious s
        JOIN project.accounts a ON s.client = a.client
        JOIN project.clients cl ON a.client = cl.client_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM project.rep_fraud r
            WHERE r.event_dt = s.event_dt
              AND r.passport = cl.passport_num
              AND r.event_type = 'Операции в разных городах в течение часа'
        )
        """
    )
    conn.commit()

def process_file(file_path, loader_func):
    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        return

    try:
        # Загружаем данные
        loader_func(file_path)

        # Переименовываем файл с расширением .backup
        base_name = os.path.basename(file_path)
        backup_name = base_name + ".backup"
        backup_path = os.path.join(input_folder, backup_name)
        os.rename(file_path, backup_path)

        # Перемещаем в папку archive
        shutil.move(backup_path, os.path.join(archive_folder, backup_name))
        print(f"Файл {base_name} перемещен в архив.")
    except Exception as e:
        print(f"Ошибка обработки файла {file_path}: {e}")

# ----------------------------



# Основной блок обработки файлов(нужно прописывать дату)
# ----------------------------
if __name__ == "__main__":
    # Выбираем дату
    process_date = "01032021"

    # Список файлов с функциями загрузки
    files_to_process = [
        (f"transactions_{process_date}.csv", transaction2sql),
        (f"terminals_{process_date}.xlsx", terminal2sql),
        (f"passport_blacklist_{process_date}.xlsx", passport2sql)
    ]

    for file_name, loader_func in files_to_process:
        file_path = os.path.join(input_folder, file_name)
        process_file(file_path, loader_func)




# drop_stg_tables()
# drop_dwh_and_rep_tables()

create_dwh_fact_transaction()
add_dwh_fact_transaction()
create_dwh_fact_passport_blacklist()
add_dwh_fact_passport_blacklist()
create_dwh_dim_terminal_hist()
create_v_terminal()
insert_new_terminals()
update_changed_terminals()
mark_deleted_terminals()
create_rep_fraud()
load_rep_fraud1()
load_rep_fraud2()
load_rep_fraud3()