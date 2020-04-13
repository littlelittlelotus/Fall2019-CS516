#!/use/bin/python3

import xml.sax
import psycopg2

TABLES = ['books', 'authors']

class docHandler(xml.sax.ContentHandler):
  def __init__(self):
    self.stack = []
    self.ch = ''

  def startElement(self, name, attributes):
    if name == 'book':
      self.stack.append(Book(attributes['ISBN']))
    elif name == 'author':
      book = self.stack[-1]
      self.stack.append(Author(attributes['firstname'], attributes['lastname'], book))

  def endElement(self, name):
    if name == 'title':
      book = self.stack[-1]
      book.setTitle(self.ch)
    #else:
      #obj = self.stack.pop()
      #if self.ch:
        #characters(self.ch)
      #obj.save()
    self.ch = ''

  def characters(self, ch):
    self.ch = self.ch + ch


class copyStore:
  def __init__(self):
    self.tables = {}
    self.id = 0 # TODO: get next value from database sequence

  def add(self, table, *columns):
    if not table in self.tables:
      self.tables[table] = []
    self.tables[table].append(list(map(str, columns)))

  def getId(self):
    self.id = self.id + 1
    return self.id


class Book:
  def __init__(self, isin):
    self.isin = isin
    self.id = store.getId()

  def setTitle(self, title):
    self.title = title

  def setAuthor(self, author):
    self.author = author

  def setPrice(self, price):
    self.price = price

  def save(self):
    store.add('books', self.id, self.title, self.author)


class Author:
  def __init__(self, first, last, book):
    self.first = first
    self.last = last
    book.setAuthor(self)
    self.id = store.getId()

  def save(self):
    store.add('authors', self.id, self.first, self.last)


if __name__ == '__main__':
  conn = psycopg2.connect(database="postgres",
                                    user="postgres",
                                    password="Yangxh199-")
  curs = conn.cursor()
  store = copyStore()
  parser = xml.sax.make_parser()
  parser.setContentHandler(docHandler())
  parser.parse("bookstore.xml")
  conn.commit()

