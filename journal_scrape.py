import re
import time
from random import random
from collections import defaultdict

from bs4 import BeautifulSoup as Bs
from requests import get
import pandas as pd

from dbclass import Database, Article


class JNeurophys:
    """
    This is a class that parses the Journal of Neurophysiology.
    """

    def __init__(self, db=Database()):
        self.journal = 'J Neurophysiol'
        self.start_URL = 'https://journals.physiology.org/loi/jn/group/d1940.y1940'
        self.base_URL = 'https://journals.physiology.org/toc/jn/'
        self.crawl_delay = 1
        self.db = db

    def get_volume_list(self):
        """
        Creates a list of the volumes in the journal. This list is the size of
        the number of issues. In other words, since there are six issues
        in each volume, each volume number repeats itself six times.
        """
        vol_list = []
        year_page = Bs(get(self.start_URL).text, 'html.parser')
        all_vol = year_page.find_all('a', {'href': re.compile(r'/toc/jn/'), 'class': "issue__vol-issue"})
        for v in all_vol:
            try:
                vol_str = re.search(r'Volume\s\d{1,3}', str(v)).group()
                vol_int = int(vol_str[7:])
                vol_list.append(vol_int)
            except:
                print('Error: Can''t retrieve list of volumes.')
                breakpoint()
        vol_list.sort()
        return vol_list

    def get_issue_urls(self, volume_list):
        """
        Constructs a list of URLs for the issues of the journal.
        """
        issue_list = []
        last_v = 0
        for v in volume_list:
            if v != last_v:
                issue = 1
            else:
                issue += 1
            issue_url = self.base_URL + str(v) + '/' + str(issue)
            issue_list.append(issue_url)
        return issue_list

    def get_issue_toc(self, url):
        """
        Grabs the table of contents from the current issue. 
        It returns a list with a URL for each articles.
        """
        issue_page = Bs(get(url).text, 'html.parser')
        articles = issue_page.find_all('div',
                                       {'class': 'table-of-content'})[0].\
            find_all('div', {'class': 'issue-item'})
        toc = ['https://journals.physiology.org' + a.find('a').get('href')
               for a in articles]
        return toc

    def get_metadata(self, article):
        ''' Get metadata.
            This will scrape the article for metadata and save it to
            a dictionary.
        '''
        # get the list of authors
        art_page = article.page
        loa = art_page.find('div',
                            {'class': 'accordion-tabbed loa-accordion'}).\
            find_all('div', {'class': 'accordion-tabbed__tab-mobile'})
        
        # add all the metadata fields
        self.id = self.db.get_unique_id()
        meta = article.meta
        meta['id'] = self.id
        meta['url'] = [article.url]
        meta['journal'] = self.journal
        meta['year'] = int(art_page.select('div.cover-image__details ')[0].
                           find('span', {'class': 'coverDate'}).
                           get_text()[-4:])
        meta['volume'] = int(art_page.select('div.cover-image__details ')[0].\
            find('span', {'class': 'volume'}).get_text()[7:])
        meta['issue'] = int(art_page.select('div.cover-image__details ')[0].\
            find('span', {'class': 'issue'}).get_text()[6:])
        meta['title'] = art_page.find('h1', {'class': 'citation__title'}).\
            get_text()
        meta['authors'] = [i.find('a').get_text() for i in loa]
        meta['doi'] = art_page.\
            find('a', {'class': 'epub-section__doi__text'}).get_text()
        return meta

    def get_text(self, article):
        ''' Get text.
            This will scrape the article for the different sections of
            the article, along with the text that they contain, and save
            it all in a dictionary.
        '''
        art_page = article.page
        section=defaultdict()
        section['ABSTRACT'] = art_page.\
            select('div.hlFld-Abstract div.abstractSection')[0].get_text()
        
        fulltext = art_page.find('div', {'class': 'hlFld-Fulltext'}).findChildren()
        key = 'INTRODUCTION'
        for f in fulltext:
            title = f.name
            if title=='h1':
                key = f.get_text()
                section[key] = ""
            if title=='p':
                section[key]+=f.get_text() + '\n\n'

        return section

    def get_references(self, article):
        """
        Get reference list.
        This will scrape the article for the works cited and save
        them to a dictionary.
        """
        art_page = article.page
        rlist = art_page.find('ul', {'class': 'rlist separator'}).find_all('li')
        refs = dict()
        for k, r in enumerate(rlist):
            refs[k] = dict()
            
            urls = [i.get('href') for i in r.find_all('a')]
            year = r.find('span', {'class': 'references__year'})
            title = r.find('span', {'class': 'references__article-title'})
            journal = r.find('span', {'class': 'references__source'})
            volume = r.find('i')
            authors = r.find('span', {'class': 'references__authors'})
            
            refs[k]['url'] = [i for i in urls if 'http' in i]
            refs[k]['year'] = int(year.get_text()[:4]) if year else -1
            refs[k]['title'] = title.get_text() if title else ""
            refs[k]['volume'] = -1
            if volume:
                if volume.get_text().isnumeric():
                    refs[k]['volume'] = int(volume.get_text())  
            refs[k]['journal'] = journal.get_text() if journal else ""
            refs[k]['authors'] = authors.get_text().split(', ') if authors else []
        return refs

    def get_article(self, url, art_page=""):
        ''' 
        get_article 
        Takes a URL for an article, scrapes the metadata,
        the text, and the references of that article, and then returns
        dictionaries for the metadata, text, and references.
        '''
        article = Article()
        article.url = url
        try: 
            art_page = Bs(get(url).text, 'html.parser') if not art_page else art_page
            article.page = art_page
            article.meta = self.get_metadata(article)
            article.section = self.get_text(article)
            article.refs = self.get_references(article)
            return article
        except:
            print('ERROR: ARTICLE NOT SAVED TO DB: {}'.format(article.url))
            return False
        

    def crawl_journal(self):
        """
        Starts from the beginning of the journal (subject to open-source
        constraints for now) and scrapes all of the articles.
        """
        # get all volumes
        vol_list = self.get_volume_list()
        print('journal volumes fetched')
        # delay the crawl 
        time.sleep(float(random() * self.crawl_delay + 1))
        
        # only look at open access issues for now
        open_access = [i for i in vol_list if i>76]
        iss_list = list(set(self.get_issue_urls(open_access)))
        print('journal issues fetched')
        time.sleep(float(random() * self.crawl_delay + 1))
        
        # loop through issues
        for iss in iss_list:
            print('\n********\nCurrently scraping issue at {}\n*********'.format(iss))
            # get toc for this issue
            toc = self.get_issue_toc(iss)
            
            # check the db to see if any of these articles have already been added
            sql = """
            SELECT DISTINCT(b.meta_id), m.url
            FROM body AS b
            LEFT JOIN metadata AS m on m.id = b.meta_id
            """
            data = pd.read_sql(sql, self.db.conn)
            db_urls = data['url'].apply(lambda row: row[0]).to_list()
            toc = self.get_issue_toc(iss)
            
            # only add articles that aren't already in the db
            diff_toc = [e for e in toc if e not in db_urls]
            for c in diff_toc:
                # delay the crawl 
                time.sleep(float(random() * self.crawl_delay + 1))
                # check if article is behind a paywall. If so, skip it (for now)
                art_page = Bs(get(c).text, 'html.parser')
                if art_page.find('div',{'class':'citation__access__icon icon-Icon_Permissions-Locked'}):
                    continue
                else:
                    article = self.get_article(c, art_page)
                    if article:
                        # (redundant) if article isn't already in the db, put it there
                        art_exists = self.db.article_exists(
                            article.meta['title'],
                            article.meta['year']
                        )
                        if not art_exists:
                            self.db.insert_article(article)
                        

    def set_proxy(self, proxy):
        """
        Set a proxy server to route our HTTP requests through.
        """

        self.proxy = proxy
        self.timeout = None






