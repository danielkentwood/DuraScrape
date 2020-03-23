import logging

import psycopg2 as pg

import yaml

import pandas as pd


log = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.conn = self.get_database()

    def get_database(self):
        try:
            conn = self.get_connection_from_profile()
            log.info("Connected to PostgreSQL database.")
        except IOError:
            log.exception("Failed to get database connection.")
            return None, 'fail'
        return conn

    def get_connection_from_profile(self,
                                    config_file_name="default_profile.yaml"):
        """
        Sets up database connection from config file.
        """
        with open(config_file_name, 'r') as f:
            vals = yaml.load(f)
        if not ('HOST' in vals.keys() and
                'USER' in vals.keys() and
                'PASSWORD' in vals.keys() and
                'DBNAME' in vals.keys() and
                'PORT' in vals.keys()):
            raise Exception('Bad config file: ' + config_file_name)

        return self.get_connection(vals['DBNAME'], vals['USER'],
                                   vals['HOST'], vals['PORT'],
                                   vals['PASSWORD'])

    def get_connection(self, db, user, host, port, passwd):
        """
        Get connection using credentials.
        """
        conn_string = "host={} port={} dbname={} user={} password={}".\
            format(host, str(port), db, user, passwd)
        return pg.connect(conn_string)

    def terminate_all_other_connections(self):
        sql="""
        SELECT pg_terminate_backend( pid ) \
        FROM pg_stat_activity \
        WHERE pid <> pg_backend_pid( ) \
        AND datname = current_database( );
        """
        with self.db.conn.cursor() as cursor:
            cursor.execute(sql)

    def create_tables(self):
        with self.conn.cursor() as cursor:
            meta_sql = '''
                CREATE TABLE IF NOT EXISTS metadata (
                id bigint primary key,
                url text[],
                journal text,
                year int,
                volume int,
                issue int,
                title text,
                authors text[],
                doi text,
                citations integer[],
                cited_by integer[]
            );'''
            cursor.execute(meta_sql)
            text_sql = '''
                CREATE TABLE IF NOT EXISTS body (
                id bigint,
                sectionA text,
                sectionB text,
                prose text
            );'''
            cursor.execute(text_sql)
        self.conn.commit()


class Article():
    def __init__(self):
        self.meta = self.initialize_meta()
        self.section = {}
        self.refs = {}
        self.url = ''
        self.page = ''
        self.db = Database()

    def initialize_meta(self):
        meta = {}
        meta['id'] = -1
        meta['url'] = []
        meta['journal'] = ''
        meta['year'] = -1
        meta['volume'] = -1
        meta['issue'] = -1
        meta['title'] = ''
        meta['authors'] = []
        meta['doi'] = ''
        meta['citations'] = []
        meta['cited_by'] = []
        return meta

    def get_unique_id(self):
        sql = """\
        SELECT MAX(id) \
        FROM metadata"""
        with self.db.conn.cursor() as cursor:
            cursor.execute(sql)
            uid = cursor.fetchone()[0]
            if not isinstance(uid,int):
                return 0
            else:
                return uid + 1

    def article_exists(self, title, year):
        sql_command = """\
        SELECT COUNT(*) \
        FROM metadata \
        WHERE title = %s \
        AND year = %s"""
        args = (title, year)
        with self.db.conn.cursor() as cursor:
            cursor.execute(sql_command, args)
            return cursor.fetchone()[0] > 0

    def get_article_list(self, journal_name):
        sql_command = """\
        SELECT year, issue, title \
        FROM metadata \
        WHERE journal = {}"""
        args = "'" + journal_name + "'"
        data = pd.read_sql(sql_command.format(args), db.conn)
        return data

    def get_meta_from_title_and_year(self, title, year):
        sql_command = """\
        SELECT * \
        FROM metadata \
        WHERE title = %s \
        AND year = %s"""
        args = (title, year)
        meta_keys = ['id', 'url', 'journal', 'year',
                     'volume', 'issue', 'title', 'authors',
                     'doi', 'citations', 'cited_by']
        with self.db.conn.cursor() as cursor:
            cursor.execute(sql_command, args)
            meta_vals = list(cursor.fetchone())
        meta = dict(zip(meta_keys, meta_vals))
        return meta

    def insert_metadata(self, meta):
        sql_command = """\
        INSERT INTO metadata (\
        id,url,journal,year,volume,issue,title,authors,doi,citations,cited_by\
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        args = (meta['id'], meta['url'], meta['journal'], meta['year'], meta['volume'],
                meta['issue'], meta['title'], meta['authors'], meta['doi'],
                meta['citations'], meta['cited_by'])
        with self.db.conn.cursor() as cursor:
            cursor.execute(sql_command, args)
        self.db.conn.commit()

    def insert_citations(self):
        ref_id_list = []
        # Loop through the references
        for c in self.refs:
            # For each ref, see if it exists. If not, build it and insert.
            # If it does exist, grab the metadata
            if not self.article_exists(self.refs[c]['title'], 
                                       self.refs[c]['year']):
                meta = self.initialize_meta()
                meta['id'] = self.get_unique_id()
                meta['url'] = self.refs[c]['url']
                meta['year'] = self.refs[c]['year']
                meta['title'] = self.refs[c]['title']
                meta['journal'] = self.refs[c]['journal']
                meta['authors'] = self.refs[c]['authors']
                self.insert_metadata(meta)
            else:
                meta = self.get_meta_from_title_and_year(self.refs[c]['title'], 
                                                         self.refs[c]['year'])
            # Add the current article's id to cited article's metadata
            sql = """\
            UPDATE metadata \
            SET cited_by = array_cat(cited_by, ARRAY[%s]) \
            WHERE id = %s"""
            args = (self.meta['id'], meta['id'])
            with self.db.conn.cursor() as cursor:
                cursor.execute(sql, args)
            ref_id_list.append(meta['id'])
        # Now add the list of cited ids to the current article's metadata
        sql = """\
        UPDATE metadata \
        SET citations = array_cat(citations, %s) \
        WHERE id = %s"""
        print(self.meta['id'], meta['id'])
        args = (ref_id_list, self.meta['id'])
        with self.db.conn.cursor() as cursor:
            cursor.execute(sql, args)
        self.db.conn.commit()

    def insert_text(self):
        for key in self.section.keys():
            sql_command = """\
            INSERT INTO body (\
            id,sectiona,prose\
            ) VALUES (%s, %s, %s)"""
            args = (self.meta['id'], key, self.section[key])
            with self.db.conn.cursor() as cursor:
                cursor.execute(sql_command, args)
        self.db.conn.commit()

    def pandas_metadata(self):
        sql_command = "SELECT * FROM metadata"
        data = pd.read_sql(sql_command, self.db.conn)
        return data

    def pandas_body(self):
        sql_command = "SELECT * FROM body"
        data = pd.read_sql(sql_command, self.db.conn)
        return data

    def drop_empty_text_sections(self):
        """
        Find and drop sections in the body table 
        that have no text in them.
        """
        sql = """
        DELETE FROM body \
        WHERE COALESCE(prose, '') = '';
        """ 
        with self.db.conn.cursor() as cursor:
            cursor.execute(sql)
        self.db.conn.commit()















    