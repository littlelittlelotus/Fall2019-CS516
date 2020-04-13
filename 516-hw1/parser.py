#!/usr/bin/python3

"""
-*- coding: utf-8 -*-
Author: Xiaohe Yang
DUKE University
Computer Science 516 Database Systems

Reference Used:
https://gist.github.com/mocobeta/6d13a4f4982f448c6b5f
http://mclassen.de/articles/bulk-loading-xml-encoded-data-into-postgresql.html
https://www.tutorialspoint.com/python/python_xml_processing.htm
https://www.youtube.com/watch?v=JJFYeazDqMU
https://www.tutorialspoint.com/python/python_xml_processing.htm
"""

import xml.sax
import psycopg2

# Handler for processing "inproceedings" and "article"
class docHandler(xml.sax.ContentHandler):
    # enable psycopg2 connection by adding connection variable in init
    def __init__(self,conn):
        # attributes variables
        self.CurrentData = ""
        self.pubkey = ""
        self.title = ""
        self.booktitle = ""
        self.journal = ""
        self.year = ""
        # counter to control number of commits
        self.counter = 0
        # connection using psycopg
        self.conn = conn
        self.cur = self.conn.cursor()

    def startElement(self,tag,attributes):
        self.CurrentData = tag
        if tag == "inproceedings" or tag == "article":
            self.pubkey = attributes["key"]

    def endElement(self,tag):
        if tag == "inproceedings":
            # check for dupkicates by sql SELECT, will be NULL is not exist
            self.cur.execute("SELECT * FROM inproceedings WHERE pubkey = %s;", (self.pubkey,))
            fileExist = self.cur.fetchone()
            # if this is the first occurance, sql INSERT into Postgres
            if not fileExist:
                #print("!!!", self.pubkey, self.title,self.booktitle,self.year)
                self.cur.execute("INSERT INTO inproceedings VALUES (%s,%s,%s,%s)",
                                 (self.pubkey, self.title, self.booktitle, self.year))
                self.counter+=1
        elif tag == "article":
            self.cur.execute("SELECT * FROM article WHERE pubkey = %s;", (self.pubkey,))
            fileExist = self.cur.fetchone()
            if not fileExist:
                self.cur.execute("INSERT INTO article VALUES (%s,%s,%s,%s)",
                                 (self.pubkey, self.title, self.journal, self.year))
                self.counter += 1

        self.CurrentData = ""
        # set counter to commit every 10000 INSERT
        if self.counter > 10000:
            self.conn.commit()
            self.counter = 0

    def characters(self,content):
        if self.CurrentData == "title":
            self.title = content
        if self.CurrentData == "booktitle":
            self.booktitle = content
        if self.CurrentData == "journal":
            self.journal = content
        if self.CurrentData == "year":
            self.year = content


# handler for processing "authorship"
class authorHandler(xml.sax.ContentHandler):
    def __init__(self, conn):
        self.CurrentData = ""
        self.pubkey = ""
        self.title = ""
        # use lists to store elements seperated by a space
        self.author = []
        self.subauthor = []
        # counter to control number of commits
        self.counter = 0
        self.counterInsert = 0
        # connection using psycopg
        self.conn = conn
        self.cur = self.conn.cursor()

    def startElement(self, tag, attributes):
        self.CurrentData = tag
        if tag == "inproceedings" or tag == "article":
            self.pubkey = attributes["key"]

    def endElement(self, tag):
        # the end of "author" element
        # should have a lists of subauthor, individual strings seperated by a space
        if tag == "author":
            temp = ""
            for sub in self.subauthor:
                temp = temp + sub
            self.author.append(temp)
            self.subauthor = []
        # the end of "article" or "inproceedings" element
        # need to INSERT here to Postgres
        if tag == "inproceedings" or tag == "article":
            for singleAuthor in self.author:
                #print("singleAuthor", singleAuthor, self.pubkey)
                # check for duplicate entries
                # only duplicate if both primary keys (author, pubkey) are identical to existing
                self.cur.execute("SELECT * FROM authorship WHERE author = %s and pubkey = %s;", (singleAuthor,self.pubkey))
                fileExist = self.cur.fetchone()
                if not fileExist:
                    #print("!~",self.pubkey)
                    self.cur.execute("INSERT INTO authorship VALUES (%s,%s)",(self.pubkey, singleAuthor))
                    self.counterInsert += 1
                    #number of inserts may be much larger due to special symbols
                self.counter+=1
            self.author = []

        # reset counters
        self.CurrentData = ""
        if self.counter > 10000:
             self.conn.commit()
             print("commited! Insert #:", self.counterInsert)
             self.counter = 0
             self.counterInsert=0

    # not assign values, only append to the end of subauthor
    def characters(self,content):
        if self.CurrentData == "author"  and self.pubkey != "":
            self.subauthor.append(content)


# parse the file twice into Postgres, since "authorship" requires special handling
# so "authorship" has different characters() function in psycopg2 setup
if ( __name__ == "__main__"):

    conn = psycopg2.connect(host="localhost",
                            database="dblp",
                            user="postgres",
                            password="Yangxh199-")
    cur = conn.cursor
    # create an XMLReader
    parser = xml.sax.make_parser()
    # turn off namepsaces
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)

    # override the default ContextHandler
    Handler = docHandler(conn)
    parser.setContentHandler( Handler )
    parser.parse("dblp.xml")
    conn.commit()
    conn.close()


    conn = psycopg2.connect(host="localhost",
                            database="dblp",
                            user="postgres",
                            password="Yangxh199-")

    # create an XMLReader
    parser = xml.sax.make_parser()
    # turn off namepsaces
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)
    # parse again using handler specific for author
    Handler = authorHandler(conn)
    parser.setContentHandler(Handler)
    parser.parse("dblp.xml")
    conn.commit()
    conn.close()
