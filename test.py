from collections import OrderedDict
from django.apps import apps
from django.conf import settings
import subprocess
import re
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connections
import json
import os
from sherlock_mt.settings import BASE_DIR
import configparser
import pymysql

def copy_client_master_tbls(new_app):
    '''
    mysql_conn = pymysql.connect(user='perceptiviti',
                    password='P01012020',
                    host='172.19.6.209',
                    database='sherlock_nlginas')

    mysql_cursor = mysql_conn.cursor()

    query = 'show tables'
    # Get All Tables
    mysql_cursor.execute(query)
    tables = mysql_cursor.fetchall()

    omit_table = {"nlginas_enginefeedback","nlginas_enginefeedbackamc","nlginas_claimrule","nlginas_mndflagging"}

    for table in tables:
        table_name = table[0]
        # sherlock_table_name = table_name.replace('masterdata', 'sherlock')
        if table_name.startswith('#'): 
            # 2 table tableau and 6 table of engine feedback
            continue
        if table_name.startswith('nlginas_') and table_name not in omit_table:
            query = f'INSERT INTO sherlock_{new_app}.{new_app}_{table_name[8:]} SELECT * FROM sherlock_nlginas.{table_name}'
            # print(query)
            try:
                print(f"Inserted table sherlock_nlginas.{table_name} into sherlock_{new_app}.{new_app}_{table_name[8:]}")
                mysql_cursor.execute(query)
                mysql_conn.commit()
            except Exception as e:
                print(e)
                print(table_name)

    mysql_cursor.close()
    mysql_conn.close()
    '''
    mysql_conn = pymysql.connect(user='perceptiviti',
                    password='P01012020',
                    host='172.19.6.209',
                    database='sherlock_nlginas')

    mysql_cursor = mysql_conn.cursor()

    query = 'show tables'
    # Get All Tables
    mysql_cursor.execute(query)
    tables = mysql_cursor.fetchall()

    omit_table = {"nlginas_enginefeedback","nlginas_enginefeedbackamc","nlginas_claimrule","nlginas_mndflagging"}

    for table in tables:
        table_name = table[0]
        if table_name.startswith('#') or '_bkp_' in table_name or '_bkp' in table_name or '_bk_' in table_name or 'nirmal' in table_name or 'may23' in table_name or 'aayushi' in table_name or 'covidcodes' in table_name or 'p_to_p_cci_unbundle' in table_name or '16092024' in table_name or 'rule_initial'in table_name or 'rule_new' in table_name or 'sample' in table_name or 'test' in table_name or 'nlginas_claimfile' in table_name or 'fileerrors' in table_name:
            continue
        if table_name.startswith('nlginas_') and table_name not in omit_table:

            query = f'desc sherlock_nlginas.{table_name}'
            mysql_cursor.execute(query)
            columns = mysql_cursor.fetchall()

            fields = ', '.join([row[0] for row in columns if row[0] != 'id'])

            query = f'delete from sherlock_{new_app}.{new_app}_{table_name[8:]}'
            mysql_cursor.execute(query)
            mysql_conn.commit()

            # query = f'INSERT INTO sherlock_{new_app}.{new_app}_{table_name[8:]} SELECT * FROM sherlock_nlginas.{table_name}'
            query = f'insert into sherlock_{new_app}.{new_app}_{table_name[8:]} ({fields}) select {fields} from sherlock_nlginas.{table_name}'
            try:
                print(query)
                mysql_cursor.execute(query)
                mysql_conn.commit()
            except pymysql.err.ProgrammingError as e:
                print(f"Skipping {table_name} because : {e}")
                continue

    mysql_cursor.close()
    mysql_conn.close()

def create_main_tables(new_app):
    # mysql_conn = connections['default'].connect()
    mysql_conn = pymysql.connect(user='perceptiviti',
                    password='P01012020',
                    host='172.19.6.209',
                    database='sherlock_nlginas')

    mysql_cursor = mysql_conn.cursor()

    query = 'show tables'
    # Get All Tables
    mysql_cursor.execute(query)
    tables = mysql_cursor.fetchall()

    omit_table = {"nlginas_flagging_analysis","nlginas_total_flagged","claim_summary",
                "drg_data_2023","masterdata_dental_icd_final","temp_obs_data","topcaremc_resub","daily_data","claim_uniqueids","activity_uniqueids"}

    for table in tables:
        table_name = table[0]
        if table_name.startswith('#') or '_bkp_' in table_name or '_bk_' in table_name or 'nirmal' in table_name: 
            continue
        if table_name.startswith('nlginas_'):
            continue
        if table_name in omit_table:
            continue
        # query = f'CREATE TABLE sherlock_{new_app}.{table_name} AS SELECT * FROM sherlock_nlginas.{table_name} LIMIT 0'
        query = f'CREATE TABLE sherlock_{new_app}.{table_name} LIKE sherlock_nlginas.{table_name} '

        mysql_cursor.execute(query)
        print("created table - ", table_name)

    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()

def create_stg_tables(new_app):
    mysql_conn = pymysql.connect(user='perceptiviti',
                    password='P01012020',
                    host='172.19.6.209',
                    database='stg_nlginas')

    # mysql_conn = connections['default'].connect()
    mysql_cursor = mysql_conn.cursor()
    query = 'show tables'

    # Get All Tables
    mysql_cursor.execute(query)
    tables = mysql_cursor.fetchall()
    omit_table = {"stg_remittance_13oct2022"}

    for table in tables:
        table_name = table[0]
        if table_name in omit_table:
            continue
        
        query = f'CREATE TABLE stg_{new_app}.{table_name} like stg_nlginas.{table_name}'
        mysql_cursor.execute(query)
        
        print("created table - ", table_name)

    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()

def create_dbs_tbls(new_app):
    print("Creating staging, main dbs & tables")
    conn = connections['default']
    conn.connect()
    cursor = conn.cursor()
    
    dbs = [f'sherlock_{new_app}', f'stg_{new_app}']
    sql_queries = [f'create database {db}' for db in dbs]

    # executing query to add databases
    for queries in sql_queries:
        cursor.execute(queries)

    conn.commit()

    for row in cursor:
        print(row)
    
    cursor.close()
    conn.close()

    # tbls = [f'sherlock_{new_app}.', f'stg_{new_app}.claim'] # put all table names here
    # sql_queries = [f'create table {tbl}' for tbl in tbls]

    # for queries in sql_queries:
    #     cursor.execute(queries)
    
    create_stg_tables(new_app)
    create_main_tables(new_app)
    print("tables created")

   
def create_cnf_file(file_path,new_app,db):
    """
    Creates a .cnf file with the specified configuration data.
    Parameters:
    - file_path (str): The path where the .cnf file will be created.
    """
    config_data = {
        'client': {
            'database' : f'''{db}_{new_app}''',
            'user': 'perceptiviti',
            'password': 'P01012020',
            'default-character-set' : 'utf8',
            'host': '172.19.6.209'
        }
    }
    config = configparser.ConfigParser()

    # Add each section and its corresponding key-value pairs
    for section, params in config_data.items():
        config[section] = params

    # Write the configuration to the specified file
    with open(file_path, 'w') as configfile:
        config.write(configfile)

    print(f"{file_path} file created and data written successfully.")

# settings.INSTALLED_APPS += (new_app_name, )

# data = {}

# app_name = 'xmlclient'

# if app_name not in settings.INSTALLED_APPS:
#     settings.INSTALLED_APPS += (app_name, )

# data['INSTALLED_APPS'] = settings.INSTALLED_APPS
# print(json.dumps(settings.DATABASES, indent=4))
# data['DATABASES'] = {}
# for db, db_config in settings.DATABASES.items():
#     data['DATABASES'][db] = {}
#     data['DATABASES'][db]['ENGINE'] = db_config['ENGINE']
#     data['DATABASES'][db]['OPTIONS'] = db_config['OPTIONS']
#     data['DATABASES'][db]['OPTIONS']['read_default_file'] = data['DATABASES'][db]['OPTIONS']['read_default_file'].replace(str(settings.BASE_DIR) + '/', '')


# with open(os.path.join(settings.BASE_DIR,
#                        'sherlock_mt',
#                        'settings.json'), 'w') as settings_file:
#     json.dump(data, settings_file, indent=4)

def commented_by_shivam_fr_settingspy(new_app):
    print("Updating dbs in settings.py")
    data = {}

    if new_app not in settings.INSTALLED_APPS:
        settings.INSTALLED_APPS += (new_app, )

    data['INSTALLED_APPS'] = settings.INSTALLED_APPS

    print("App added ")
    db_types=['stg','db']

    if new_app not in settings.DATABASES:
        for db_type in db_types:
            new_db_name = f'''{new_app}_{db_type}'''
            new_db_config = {
                'ENGINE': 'django.db.backends.postgresql',  # Example engine, replace as needed
                'OPTIONS': {
                    'read_default_file': f""".{new_app}_{db_type}.cnf"""
                }
            }
            settings.DATABASES[new_db_name] = new_db_config  # Add to settings in memory
        print(f"Added {new_db_name} to DATABASES")

    ##

    data['DATABASES'] = {}
    for db, db_config in settings.DATABASES.items():
        print("Iteration")
        print("value of db : ",db," and db_config : ",db_config)

        data['DATABASES'][db] = {}
        data['DATABASES'][db]['ENGINE'] = db_config['ENGINE']
        data['DATABASES'][db]['OPTIONS'] = db_config['OPTIONS']
        data['DATABASES'][db]['OPTIONS']['read_default_file'] = data['DATABASES'][db]['OPTIONS']['read_default_file'].replace(str(settings.BASE_DIR) + '/', '')
        print("data['DATABASES']: ",data["DATABASES"])
        print()

    print()
    print("whole data['DATABASES'] : ",data["DATABASES"])

    print("Databases added ")


    new_router = f'''sherlock_mt.routers.{capitalize_first_letter(new_app)}Router'''
    # if not hasattr(settings, 'DATABASE_ROUTERS'):
    #     settings.DATABASE_ROUTERS = []  # Initialize if it doesn't exist

    if new_router not in settings.DATABASE_ROUTERS:
        settings.DATABASE_ROUTERS.append(new_router)  # Add to settings in memory
        print(f"Added {new_router} to DATABASE_ROUTERS")

    # Save DATABASE_ROUTERS to data dictionary for JSON output
    data['DATABASE_ROUTERS'] = settings.DATABASE_ROUTERS

    print("Routers added ")

    with open(os.path.join(settings.BASE_DIR,
                           'sherlock_mt',
                           'settings.json'), 'w') as settings_file:
        json.dump(data, settings_file, indent=4)

    print("Settings updated and saved to settings.json")

def copy_files_with_cp(src_dir, dst_dir, files_to_copy,new_app ):
    """
    Copies specific files from src_dir to dst_dir using the cp command, and replaces old_str with new_str in the file contents.

    :param src_dir: The directory of the source app.
    :param dst_dir: The directory of the destination app.
    :param files_to_copy: List of filenames to be copied.
    """
    new_str = new_app
    old_str = 'nlginas'
    for file_name in files_to_copy:
        src_file = f"{src_dir}/{file_name}"
        dst_file = f"{dst_dir}/{file_name}"
        
        try:
            # Read the content of the source file
            with open(src_file, 'r') as f:
                content = f.read()

            # Replace occurrences of old_str in identifiers (e.g., table or field names)
            # Allows for replacements in both quoted and unquoted uses of old_str in SQL queries

            # pattern = r"(?<!\w)" + re.escape(old_str) + r"(?!\w)"
            pattern = re.escape(old_str)  # Match 'nlginas' anywhere in text
            content = re.sub(pattern, new_str, content)

            # Write the modified content to the destination file
            with open(dst_file, 'w') as f:
                f.write(content)

            print(f"Copied and modified {file_name} to {dst_dir}")
        except FileNotFoundError:
            print(f"File {file_name} not found in {src_dir}")
        except Exception as e:
            print(f"Failed to copy and modify {file_name} from {src_dir} to {dst_dir}: {e}")

def create_client(new_app):

    if new_app in settings.INSTALLED_APPS:
        return f"{new_app} already exists"
    # client app created
    try:
        call_command('startapp', new_app) 
    except CommandError as e:
        print(e)
        return e

    # entry in settings.json.installed_apps 
    # settings.INSTALLED_APPS += (new_app, )
    # apps.app_configs = OrderedDict()
    # apps.apps_ready = apps.models_ready = apps.loading = apps.ready = False
    # apps.clear_cache()
    # apps.populate(settings.INSTALLED_APPS)
    # settings_data = {}
    # settings_data['INSTALLED_APPS'] = settings.INSTALLED_APPS


    # # commented_by_shivam_fr_settingspy(new_app)
    
    # create dbs,stg-main tables
    create_dbs_tbls(new_app)
    
    # copy files from nlginas to new app
    src_directory = os.path.join(BASE_DIR, "nlginas")
    dst_directory = os.path.join(BASE_DIR, new_app)

    # List of specific files you want to copy
    files = ["models.py", "views.py", "urls.py","processdoc.py",\
             "data_insertion_config.py","data_insertion_daily.py", \
             "data_insertion_utils.py","serializers.py","file_config.py", \
             "excel_validation.py","reports.py","reports_constants.py",
             "generate_output.py","engine_feedback_response.py"]  

    #copy_files_with_cp(src_directory, dst_directory, files,new_app)

    # edit settings.json

    # create config file
    create_cnf_file(f'''.{new_app}_stg.cnf''',new_app,'stg')
    create_cnf_file(f'''.{new_app}_db.cnf''',new_app,'sherlock')
    

    # Call the make migrations & migrate
    # call_command('makemigrations', new_app)
    # call_command('migrate', new_app, f'''{new_app}_db''')

    # run this function this after manual make migrations, migrate
    # copy_client_master_tbls(new_app)

print(create_client('oman'))

# print(create_dbs_tbls('pllt'))

# print(copy_client_master_tbls('oman'))

