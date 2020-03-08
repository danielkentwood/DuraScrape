import os

import yaml

import psycopg2 as pg

import logging


log = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.conn = get_database()
        create_tables(self)

    def get_database()
        try:
            conn = get_connection_from_profile()
            log.info("Connected to PostgreSQL database.")
        except IOError:
            log.exception("Failed to get database connection.")
            return None, 'fail'
        return conn

    def get_connection_from_profile(config_file_name="default_profile.yaml"):
        """
        Sets up database connection from config file.
        Input:
        config_file_name: File containing PGHOST, PGUSER,
                          PGPASSWORD, PGDATABASE, PGPORT, which are the
                          credentials for the PostgreSQL database
        """
        with open(config_file_name, 'r') as f:
            vals = yaml.load(f)
        if not ('HOST' in vals.keys() and
                'USER' in vals.keys() and
                'PASSWORD' in vals.keys() and
                'DBNAME' in vals.keys() and
                'PORT' in vals.keys()):
            raise Exception('Bad config file: ' + config_file_name)

        return get_connection(vals['DBNAME'], vals['USER'],
                          vals['HOST'], vals['PORT'],
                          vals['PASSWORD'])

    def get_connection(db, user, host, port, passwd):
        """
        Get connection using credentials.
        Input:
        db: database name
        user: Username
        host: Hostname of the database server
        port: Port number
        passwd: Password for the database
        """
        conn_string = "host="+ host +" port="+ port +" dbname="+ db \
            +" user=" + user +" password="+ passwd
        return psycopg2.connect(conn_string)

    def create_tables(self):
        meta_sql = '''
            CREATE TABLE IF NOT EXISTS metadata (
            id int primary key,
            url text,
            journal text,
            year int,
            volume int,
            issue int,
            title text,
            authors text[],
            doi text,
            citations integer[],
            cited_by integer[],
        );'''
        text_sql = '''
            CREATE TABLE IF NOT EXISTS metadata (
            id int,
            sectionA text,
            sectionB text,
            text text
        );'''




    def get_unique_id(self):
        dummy = ''

    def new_article_check(self):
        dummy = ''

    def add_citation_to_db(self, citing_id, cited_id):
        dummy = ''

    def add_new_article(self, meta, section, refs):
        # add metadata
        with self.conn.cursor() as cursor:
            sql = "INSERT INTO 'Metadata' ('id',\
            'url','volume','issue','title','authors',\
            'doi') VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, args)
        # add text by section

        # add references


                if not ('HOST' in vals.keys() and
                'USER' in vals.keys() and
                'PASSWORD' in vals.keys() and
                'DBNAME' in vals.keys() and
                'PORT' in vals.keys()):
            raise Exception('Bad config file: ' + config_file_name)