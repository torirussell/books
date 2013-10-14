# books.py
# Tori Scallan
#!/usr/bin/python

from urllib2 import urlopen
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
from pymongo import MongoClient

def setUpDB(collection_name):
  client = MongoClient() # Uses defaults from config file
  db = client['books_database']
  db_collection = db[collection_name]
  return db_collection

def insertIntoDB(db, item):
  try:
    if db.find({'reader.description':item['reader']['description']}).count() > 0:
      print '{0} already exists in {1}'.format(str(item), str(db))
    else: # Insert item into DB if it doesn't already exist there
      db.insert(item)
      print 'Inserted {0} into {1}.'.format(str(item), str(db)) # TODO: Print all of these to log file
  except:
    print 'Failed to insert {0} into {1}.'.format(str(item), str(db))

# Parses captions of form: Book_Title, Book_Author (Reader_Sex, Reader_Age, Reader_Description) Book_Url
def parseCaption(caption, reader_location='NY'):
  book_author = book_title = book_url = reader_sex = reader_age = reader_description = None
  first_paren = caption.split('(')
  if len(first_paren) > 1:
    book_info = first_paren[0].strip()
    book_list = book_info.split(',')
    if len(book_list) > 1: # TODO: check for common name suffixes & split in different place if found
      book_author = book_list[-1].strip()
      book_title = ','.join(book_list[:-1]).strip()

    second_paren = first_paren[1].split(')')
    if len(second_paren) > 1:
      reader_info = second_paren[0].strip()
      book_url = second_paren[1].strip()

      reader_list = reader_info.split(',')
      if len(reader_list) > 2:
        reader_sex = reader_list[0].strip()
        reader_age = reader_list[1].strip() # TODO: generalize to decade, not early/late
        reader_description = ','.join(reader_list[2:]).strip()

        if book_author and book_title and book_url and reader_sex and reader_age and reader_description and reader_location:
          caption_info = {
            'book': {
              'author':book_author,
              'title':book_title,
              'url':book_url
              },
            'reader': {
              'sex':reader_sex,
              'age':reader_age,
              'description':reader_description,
              'location':reader_location
              }
            }

          return caption_info

def scrapeCoverSpy(database, start_page = 0, end_page = 1, location='NY'):
  ny_url = 'http://coverspy.tumblr.com'
  sf_url = 'http://coverspysf.tumblr.com'
  page = start_page # page = 0 = homepage; page = 1 = homepage/page/2

  if location == 'NY':
    url = cs_url = ny_url
  elif location == 'SF':
    url = cs_url = sf_url

  while page < end_page:
    if page > 0:
      url = cs_url + '/page/' + str(page + 1)
    
    cover_spy = urlopen(url).read() # Get string form of page
    soup_strainer = SoupStrainer(attrs={'class':'caption'}) # Only grab captions
    soup = BeautifulSoup(cover_spy, parse_only=soup_strainer) # String -> HTML
    html_captions = soup.find_all(attrs={'class':'caption'}) # Generate list of captions

    if len(html_captions) < 1: # Assume that no captions means empty page
      break # Which means we've reached the end of content; stop processing

    # Generate list containing text element from each html caption & then
    # Call parseCaption on each text element in that list
    #caption_dicts = map(parseCaption, [caption.p.text for caption in html_captions])
    caption_dicts = [parseCaption(cap, location) for cap in [caption.p.text for caption in html_captions]]
    filtered_dicts = list(filter(None, caption_dicts)) # Get rid of empties
    # Now throw each successful caption dictionary into Mongo
    [insertIntoDB(database, capt_dict) for capt_dict in filtered_dicts]
    page += 1

def constructQueryParams(author = None, title = None, url = None, 
  sex = None, age = None, description = None, location = None):
  query_params = {}

  if author:
    query_params['book.author'] = author
  if title:
    query_params['book.title'] = title
  if url:
    query_params['book.url'] = url
  if sex:
    query_params['reader.sex'] = sex
  if age:
    query_params['reader.age'] = age
  if description:
    query_params['reader.description'] = description
  if location:
    query_params['reader.location'] = location

  return query_params

# Create (Title, Author) tuple key & Num_Readers value
def retrieveFromDB(database, query_params):
  found_books = {}

  for post in database.find(query_params):
    book_tuple = (post['book']['title'], post['book']['author'])
    if book_tuple in found_books:
      found_books[book_tuple] += 1
    else:
      found_books[book_tuple] = 1

  return found_books

# Print list of Book_Title - Book_Author : Num_Readers
def printBookCounts(books):
  for book in books.keys():
    title, author = book
    print title, "-", author, ":", books[book]

coverspy_db = setUpDB('captions_db') # Consistent DB has all CoverSpy books scraped
qp = constructQueryParams(age='20s', sex='F')
#book_dict = retrieveFromDB(coverspy_db, qp)

# Store book information for specified page range
scrapeCoverSpy(coverspy_db, location='SF')

# Print books matching query specified in qp
#printBookCounts(book_dict)

# TODO: update DB values from script

