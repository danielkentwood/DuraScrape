import re

from bs4 import BeautifulSoup as Bs

from dbclass import Article

from requests import get


class JNeurophys:
    """
    This is a docstring for the class that parses the Journal of Neuro
    psychology.
    """

    def __init__(self):
        self.journal = 'J Neurophysiol'
        self.start_URL = 'https://journals.physiology.org/loi/jn/group/d1940.y1940'
        self.base_URL = 'https://journals.physiology.org/toc/jn/'
        self.crawl_delay = 1

    def get_volume_list(self):
        """
        Creates a list of the volumes in the journal. This list is the size of
        the number of issues. In other words, since there are six issues
        in each volume, each volume number repeats itself six times.
	 """
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
            issue_url = self.base_url + str(v) + '/' + str(issue)
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
        self.id = article.get_unique_id()
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
        section = dict()
        section['ABSTRACT'] = art_page.\
            select('div.hlFld-Abstract div.abstractSection')[0].get_text()
        section['INTRODUCTION'] = ''
        fulltext = art_page.\
            find('div', {'class': 'hlFld-Fulltext'}).findChildren(recursive=False)
        intro_flag = 1
        for f in fulltext:
            heading = f.find('h1', {'class': 'article-section__title section__title'})
            if not heading:
                section['INTRODUCTION'] = section['INTRODUCTION'] + ' ' + f.get_text()
            else:
                intro_flag = 0
                section[heading.get_text()] = ''
                for text in heading.find_parent().findChildren('div'):
                    section[heading.get_text()] = section[heading.get_text()] + text.get_text()
        if not intro_flag:
            del section['INTRODUCTION']
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
            refs[k]['url'] = [i for i in urls if 'http' in i]
            refs[k]['year'] = int(r.find('span', {'class': 'references__year'}).get_text()[:4])
            refs[k]['title'] = r.\
                find('span', {'class': 'references__article-title'}).get_text()
            refs[k]['journal'] = r.find('span', {'class': 'references__source'}).get_text()
            refs[k]['authors'] = r.find('span', {'class': 'references__authors'}).\
                get_text().split(', ')
        return refs

    def get_article(self, url):
        ''' get_article takes a URL for an article, scrapes the metadata,
            the text, and the references of that article, and then returns
            dictionaries for the metadata, text, and references.
        '''
        article = Article()
        article.url = url
        art_page = Bs(get(url).text, 'html.parser')
        article.page = art_page
        article.meta = self.get_metadata(article)
        article.section = self.get_text(article)
        article.refs = self.get_references(article)
        return article

    def crawl_journal(self):
        """
        Starts from the beginning of the journal (subject to open-source
        constraints for now) and scrapes all of the articles.
        """
        vol_list = self.get_volume_list()
        open_access = [i for i in vol_list if i>76]
        iss_list = self.get_issue_urls(open_access)
        for iss in iss_list:
            toc = self.get_issue_toc(iss)
            for c in toc:
            # check if article is behind a paywall. If so, skip it (for now)
            if not c.find('div',{'class':'badges'}).get_text():
                continue 









