import logging
from configparser import ConfigParser
from collections import defaultdict

import psycopg2 as pg
import pandas as pd


log = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.conn = self.get_database()
        
    def config(self, filename='database.ini', section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
            
        return db

    def pgrs_connect(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = self.config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database: {}'.format(params['database']))
            conn = pg.connect(**params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)
        except (Exception, pg.DatabaseError) as error:
            print(error)

        return conn

    def disconnect(self):
        if not self.conn.closed:
            self.conn.close()
            print('Connection is now closed.')
        else:
            print('Connection is already closed.')

    def get_database(self):
        try:
            conn = self.pgrs_connect()
            log.info("Connected to PostgreSQL database.")
        except IOError:
            log.exception("Failed to get database connection from .ini profile.")
            return None, 'fail'
        return conn

    def terminate_all_other_connections(self):
        sql="""
        SELECT pg_terminate_backend( pid ) \
        FROM pg_stat_activity \
        WHERE pid <> pg_backend_pid( ) \
        AND datname = current_database( );
        """
        with self.conn.cursor() as cursor:
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
                id SERIAL primary key,
                meta_id bigint,
                sectionA text,
                sectionB text,
                prose text
            );'''
            cursor.execute(text_sql)
        self.conn.commit()
        
    def list_tables(self):
        query = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE (table_schema = 'public')
        ORDER BY table_schema, table_name;
        """
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            list_tables = cursor.fetchall()

            print('** CURRENT TABLES **')
            for t_name_table in list_tables:
                print(t_name_table[1])

    def pandas_metadata(self):
        """
        Returns the 'metadata' table as a pandas dataframe.
        """
        
        sql_command = "SELECT * FROM metadata"
        df = pd.read_sql(sql_command, self.conn)
        return df

    def pandas_body(self):
        """
        Returns the 'body' table as a pandas dataframe.
        """
        
        sql_command = "SELECT * FROM body"
        df = pd.read_sql(sql_command, self.conn)
        return df    

    def get_unique_id(self):
        sql = """\
        SELECT MAX(id) \
        FROM metadata"""
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            uid = cursor.fetchone()[0]
            if not isinstance(uid,int):
                return 0
            else:
                return uid + 1

    def article_exists(self, title, year):
        """
        Given a title and a year, checks if the article exists in the database.
        """
        
        sql = """\
        SELECT COUNT(*) \
        FROM metadata \
        WHERE title = %s \
        AND year = %s"""
        args = (title, year)
        with self.conn.cursor() as cursor:
            cursor.execute(sql, args)
            return cursor.fetchone()[0] > 0

    def drop_empty_text_sections(self):
        """
        Find and drop sections in the body table 
        that have no text in them.
        """
        sql = """
        DELETE FROM body \
        WHERE COALESCE(prose, '') = '';
        """ 
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
        self.conn.commit()
        
    def get_article_list(self, journal_name):
        sql = """\
        SELECT year, issue, title \
        FROM metadata \
        WHERE journal = {}"""
        args = "'" + journal_name + "'"
        data = pd.read_sql(sql.format(args), self.conn)
        return data

    def get_meta_from_title_and_year(self, title, year):
        sql = """\
        SELECT * \
        FROM metadata \
        WHERE title = %s \
        AND year = %s"""
        args = (title, year)
        meta_keys = ['id', 'url', 'journal', 'year',
                     'volume', 'issue', 'title', 'authors',
                     'doi', 'citations', 'cited_by']
        with self.conn.cursor() as cursor:
            cursor.execute(sql, args)
            meta_vals = list(cursor.fetchone())
        meta = dict(zip(meta_keys, meta_vals))
        return meta

    def insert_metadata(self, meta):
        sql = """\
        INSERT INTO metadata (\
        id,url,journal,year,volume,issue,title,authors,doi,citations,cited_by\
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        args = (meta['id'], meta['url'], meta['journal'], meta['year'], meta['volume'],
                meta['issue'], meta['title'], meta['authors'], meta['doi'],
                meta['citations'], meta['cited_by'])
        with self.conn.cursor() as cursor:
            cursor.execute(sql, args)
        self.conn.commit()

    def insert_citations(self, article):
        ref_id_list = []
        # Loop through the references
        for c in article.refs.keys():
            # For each ref, see if it exists. If not, build it and insert.
            # If it does exist, grab the metadata
            if not self.article_exists(article.refs[c]['title'], 
                                       article.refs[c]['year']):
                meta = article.initialize_meta()
                meta['id'] = self.get_unique_id()
                meta['url'] = article.refs[c]['url']
                meta['year'] = article.refs[c]['year']
                meta['title'] = article.refs[c]['title']
                meta['volume'] = article.refs[c]['volume']
                meta['journal'] = article.refs[c]['journal']
                meta['authors'] = article.refs[c]['authors']
                self.insert_metadata(meta)
            else:
                meta = self.get_meta_from_title_and_year(article.refs[c]['title'], 
                                                         article.refs[c]['year'])
            # Add the current article's id to cited article's metadata
            sql = """\
            UPDATE metadata \
            SET cited_by = array_cat(cited_by, ARRAY[%s]) \
            WHERE id = %s"""
            args = (article.meta['id'], meta['id'])
            with self.conn.cursor() as cursor:
                cursor.execute(sql, args)
            ref_id_list.append(meta['id'])
        # Now add the list of cited ids to the current article's metadata
        sql = """\
        UPDATE metadata \
        SET citations = array_cat(citations, %s) \
        WHERE id = %s"""
        args = (ref_id_list, article.meta['id'])
        with self.conn.cursor() as cursor:
            cursor.execute(sql, args)
        self.conn.commit()

    def insert_text(self, article):
        for key in article.section.keys():
            sql = """\
            INSERT INTO body (\
            meta_id,sectiona,prose\
            ) VALUES (%s, %s, %s)"""
            args = (article.meta['id'], key, article.section[key])
            with self.conn.cursor() as cursor:
                cursor.execute(sql, args)
        self.conn.commit()
    
    def insert_article(self, article):
        self.insert_metadata(article.meta)
        self.insert_citations(article)
        self.insert_text(article)
        print('Article at {} inserted into DB.'.format(article.url))
        

class Article():
    def __init__(self):
        self.meta = self.initialize_meta()
        self.section = defaultdict()
        self.refs = {}
        self.url = ''
        self.page = ''

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


 