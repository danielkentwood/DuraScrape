import re

from bs4 import BeautifulSoup as Bs

import pymysql

from requests import get


class Database:
    def __init__(self):
        self.dbname = "findingssm"
        self.host = "findingssm.c9zjgwsivgee.us-east-2.rds.amazonaws.com"
        self.port = 3306
        self.user = "danielkentwood"
        self.password = "findingsdbsm"

    def connect(self):
        self.conn = pymysql.connect(
            host=self.host,
            user=self.user,
            port=self.port,
            passwd=self.password,
            db=self.dbname,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

    def disconnect(self):
        self.conn.close()


class Table(Database):
    def __init__(self, dbname):
        self.database = Database

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


class JNeurophys:
    def __init__(self):
        self.journal = 'j_neurophys'
        self.start_URL = \
            'https://journals.physiology.org/loi/jn/group/d1940.y1940'
        self.base_URL = 'https://journals.physiology.org/toc/jn/'
        self.database = Database()

    def get_volume_list(self):
        vol_list = []
        year_page = Bs(get(self.start_URL).text, 'html.parser')
        all_vol = year_page.find_all('a', {'href': re.compile(r'/toc/jn/')})
        for v in all_vol:
            try:
                vol_str = re.search(r'Volume\s\d{1,3}', str(v)).group()
                vol_int = int(vol_str[7:])
                vol_list.append(vol_int)
            except AttributeError:
                print('Attribute Error.')
        volume_list = vol_list.sort()
        return volume_list

    def get_issue_urls(self, volume_list):
        last_v = 0
        for v in volume_list:
            if v != last_v:
                issue = 1
            else:
                issue += 1
            issue_url = self.base_url + str(v) + '/' + str(issue)
        return issue_url

    def get_issue_toc(self, url):
        issue_page = Bs(get(url).text, 'html.parser')
        articles = issue_page.find_all('div',
                                       {'class': 'table-of-content'})[0].\
            find_all('div', {'class': 'issue-item'})
        toc = ['https://journals.physiology.org' + a.find('a').get('href')
               for a in articles]
        return toc

    def get_article(self, url):
        art_page = Bs(get(url).text, 'html.parser')
        loa = art_page.find('div',
                            {'class': 'accordion-tabbed loa-accordion'}).\
            find_all('div', {'class': 'accordion-tabbed__tab-mobile '})
        # get metadata
        meta = dict()
        meta['id'] = ''
        meta['url'] = url
        meta['volume'] = art_page.select('div.cover-image__details ')[0].\
            find('span', {'class': 'volume'}).get_text()
        meta['issue'] = art_page.select('div.cover-image__details ')[0].\
            find('span', {'class': 'issue'}).get_text()
        meta['title'] = art_page.find('h1', {'class': 'citation__title'}).\
            get_text()
        meta['authors'] = [i.find('a').get_text() for i in loa]
        meta['doi'] = art_page.\
            find('a', {'class': 'epub-section__doi__text'}).get_text()
        # get text
        section = dict()
        section['Abstract'] = art_page.\
            select('div.hlFld-Abstract div.abstractSection')[0].get_text()
        section['Introduction'] = ''
        fulltext = art_page.\
            find('div', {'class': 'hlFld-Fulltext'}).\
            findChildren(recursive=False)
        intro_flag = 1
        for f in fulltext:
            heading = f.\
                find('h1', {'class': 'article-section__title section__title'})
            if not heading:
                section['Introduction'] = \
                    section['Introduction'] + ' ' + f.find_text()
            else:
                intro_flag = 0
                section[heading.get_text()] = ''
                for text in heading.find_parent().findChildren('div'):
                    section[heading.get_text()] = \
                        section[heading.get_text()] + text.get_text()
        if not intro_flag:
            del section['Introduction']
        # get reference list
        rlist = art_page.\
            find('ul', {'class': 'rlist separator'}).find_all('li')
        refs = dict()
        for k, r in enumerate(rlist):
            refs[k] = dict()
            refs[k]['url'] = [i.get('href') for i in r.find_all('a')]
            refs[k]['title'] = r.\
                find('span', {'class': 'references__article-title'}).get_text()
            refs[k]['authors'] = r.\
                find('span', {'class': 'references__authors'}).\
                get_text().split(', ')
        return meta, section, refs
