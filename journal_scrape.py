from bs4 import BeautifulSoup as bs
from requests import get
import requests
import re
import pymysql


class database:

	def __init__(self, dbname):
		self.dbname = dbname
		# set up the Amazon RDS database here
		self.host = ''
		self.port = ''
		self.user = ''
		self.password = ''


	def connect(self):
		self.conn = pymysql.connect(
			self.host,
			user=self.user,
			port=self.port,
			passwd=self.password,
			db=self.dbname)

									





class table(database):

	def __init__(self, dbname):
		self.database = database


	def get_unique_ID(self):


	def new_article_check(self):


	def add_citation_to_DB(self, citing_ID, cited_ID):


	def add_new_article(self, args):





class j_neurophys:

	def __init__(self):
		self.journal = 'j_neurophys'
		self.start_URL = 'https://journals.physiology.org/loi/jn/group/d1940.y1940'
		self.base_URL = 'https://journals.physiology.org/toc/jn/'
		self.database = database()

	def get_volume_list(self):
		vol_list=[]
		year_page = bs(get(self.start_URL).text, 'html.parser')
		all_vol - year_page.find_all('a',{'href': re.compile(r'/toc/jn/')})
		for v in all_vol:
		    try: 
		        vol_str = re.search(r'Volume\s\d{1,3}',str(v)).group()
		        vol_int = int(vol_str[7:])
		        vol_list.append(vol_int)
		    except AttributeError:
		        print('Attribute Error.')

        volume_list = vol_list.sort()
        return volume_list

    def get_issue_URLs(self, volume_list):
    	''' Takes a list of volumes. '''
    	last_v=0
    	for v in volume_list:
		    # count the issues in each volume
		    if v!=last_v:
		        issue=1
		    else:
		        issue+=1
		    issue_url = self.base_url + str(v) +'/' + str(issue)
	    return issue_url

	def get_issue_toc(self, URL):
		issue_page = bs(get(URL).text, 'html.parser')
		articles = issue_page.find_all('div',{'class':'table-of-content'})[0].find_all('div',{'class':'issue-item'})
		toc = ['https://journals.physiology.org' + a.find('a').get('href') for a in articles]
		return toc

	def get_article(self, URL):
		art_page = bs(get(URL).text, 'html.parser')
		loa = art_page.find('div',{'class':'accordion-tabbed loa-accordion'}).find_all('div',{'class':'accordion-tabbed__tab-mobile '})
		# get metadata
		meta = dict()
		meta['url'] = URL
        meta['title']= art_page.find('h1',{'class':'citation__title'}).get_text()
        meta['authors'] = [i.find('a').get_text() for i in loa]
        meta['doi'] = art_page.find('a',{'class':'epub-section__doi__text'}).get_text()
        # get text
        section = dict()
        section{'Abstract'} = art_page.select('div.hlFld-Abstract div.abstractSection')[0].get_text()
        section['Introduction'] = ''
        intro_flag = 1
        for f in fulltext:
            heading = f.find('h1',{'class':'article-section__title section__title'})
            if not heading:
                section['Introduction']  = section['Introduction']  + ' ' + f.find_text()
            else:
            	intro_flag = 0
                section[heading.get_text()] = ''
                for text in heading.find_parent().findChildren('div'):
                    section[heading.get_text()] = section[heading.get_text()] + text.get_text()

            if not intro_flag:
            	del section['Introduction']
        # get reference list
        rlist = art_page.find('ul',{'class':'rlist separator'}).find_all('li')
        refs = dict()
        for k,r in enumerate(rlist):
        	refs[k] = dict()
            refs[k]['url'] = [i.get('href') for i in r.find_all('a')]
            refs[k]['title']= r.find('span',{'class':'references__article-title'}).get_text()
            refs[k]['authors'] = r.find('span',{'class':'references__authors'}).get_text().split(', ')

        return meta, section, refs
            







