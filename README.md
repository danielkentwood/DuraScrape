# DuraScrape

## Introduction
`DuraScrape` is a web crawler that scrapes open-access journal articles from academic journal websites and saves them in a SQL database. 

The first (and currently only) journal that is available is *Journal of Neurophysiology*. 

## Usage
The code below assumes that you have set up a postgresql server and created a database called `findings_db`. You can change the database name, of course; just make sure you edit the name in the `database.ini` file 

```python
from journal_scrape import JNeurophys
from dbclass import Database

# initialize the database
db = Database()

# create tables
# this creates two tables:
# * body (for full text)
# * metadata (for metadata and citation linking)
db.create_tables()

# create JNeurophys object
jn = JNeurophys()

# tell it to crawl the journal
jn.crawl_journal()
```

As the scraping progresses, the `crawl_journal()` method will print the URL of each article and specify whether or not it was successfully saved to the database.