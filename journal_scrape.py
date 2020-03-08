import re

from dbclass import Database

from bs4 import BeautifulSoup as Bs

import psycopg2 as pg

from requests import get


db = Database()

class JNeurophys:
    def __init__(self):
        self.journal = 'JNeurophys'
        self.start_URL = 'https://journals.physiology.org/loi/jn/group/d1940.y1940'
        self.base_URL = 'https://journals.physiology.org/toc/jn/'

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

    def get_metadata(self, art_page):
        ''' Get metadata.
            This will scrape the article for metadata and save it to 
            a dictionary.
        '''
        loa = art_page.find('div',
                            {'class': 'accordion-tabbed loa-accordion'}).\
            find_all('div', {'class': 'accordion-tabbed__tab-mobile '})
        meta = dict()
        meta['id'] = ''
        meta['url'] = url

        meta['year'] = VOID
        meta['journal'] = self.journal

        meta['volume'] = art_page.select('div.cover-image__details ')[0].\
            find('span', {'class': 'volume'}).get_text()
        meta['issue'] = art_page.select('div.cover-image__details ')[0].\
            find('span', {'class': 'issue'}).get_text()
        meta['title'] = art_page.find('h1', {'class': 'citation__title'}).\
            get_text()
        meta['authors'] = [i.find('a').get_text() for i in loa]
        meta['doi'] = art_page.\
            find('a', {'class': 'epub-section__doi__text'}).get_text()
        return meta

    def get_text(self, art_page):
        ''' Get text.
            This will scrape the article for the different sections of
            the article, along with the text that they contain, and save
            it all in a dictionary.
        '''
        section = dict()
        section['Abstract'] = art_page.\
            select('div.hlFld-Abstract div.abstractSection')[0].get_text()
        section['Introduction'] = ''
        fulltext = art_page.\
            find('div', {'class': 'hlFld-Fulltext'}).findChildren(recursive=False)
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
        return section

    def get_references(self, art_page):
        ''' Get reference list.
            This will scrape the article for the works cited and save 
            them to a dictionary.
        '''
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
        return refs

    def get_article(self, url):
        ''' get_article takes a URL for an article, scrapes the metadata,
            the text, and the references of that article, and then returns
            dictionaries for the metadata, text, and references.
        '''
        art_page = Bs(get(url).text, 'html.parser')
        meta = get_metadata(art_page)
        section = get_text(art_page)
        refs = get_references(art_page)
        return meta, section, refs
