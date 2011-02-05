import os
import aspell
import email
import nltk
import base64
import string
import time
import boto
from boto.exception import EC2ResponseError

def is_lexical(word):
    return word not in ('a', 'an', 'the', 'that', 'to')

class Indexer:

    def __init__(self, path, domain='sdbindex', bucket='sdbindex'):
        self.sdb_conn = boto.connect_sdb()
        self.s3_conn = boto.connect_s3()
        self.domain = self.sdb_conn.get_domain(domain)
        self.bucket = self.s3_conn.get_bucket(bucket)
        self.total_words = 0
        self.unique_words = set()
        self.path = path
        self.speller = aspell.Speller('lang', 'en')
        l = []
        for i in range(128,256):
            l.append(chr(i))
        self.delete_chars = ''.join(l)

    def filter(self, word):
        # some messages contain non-ascii characters
        # SDB doesn't like this so we need to strip them out for now
        word = ''.join([c for c in word if c not in self.delete_chars])
        # now lookup the word and verify it's a real word
        if word:
            if self.speller.check(word) <= 0:
                word = ''
        return word.lower()

    def get_msg(self, filename):
        fullpath = os.path.join(self.path, filename)
        fp = open(fullpath)
        msg = email.message_from_file(fp)
        fp.close()
        return msg

    def get_msg_text(self, msg):
        msg_text = ''
        if msg.is_multipart():
            for submsg in msg.get_payload():
                msg_text = self.get_msg_text(submsg)
                if msg_text:
                    break
        else:
            if msg.get_content_type() == 'text/plain':
                msg_text = msg.get_payload()
                try:
                    if msg['Content-transfer-encoding'] == 'base64':
                        msg_text = base64.b64decode(msg_text)
                except:
                    pass
        return msg_text

    def get_words(self, text):
        words = []
        if text:
            g = nltk.tokenize.word(text)
            for word in g:
                word = self.filter(word)
                if word:
                    if is_lexical(word):
                        words.append(word)
        return words

    def store_file(self, filename, keyname):
        fullpath = os.path.join(self.path, filename)
        print 'storing: %s.%s' % (self.bucket.name, keyname)
        key = self.bucket.new_key(keyname)
        key.set_contents_from_filename(fullpath, replace=False)

    def create_new_key_page(self, base_key, prev_page=0):
        page = prev_page + 1
        a = {'page': str(page)}
        self.domain.put_attributes(base_key, a, replace=False)
        return page
        
    def get_current_key_page(self, base_key):
        a = self.domain.get_attributes(base_key)
        if a.has_key('page'):
            page = int(a['page'])
        else:
            page = self.create_new_key_page(base_key)
        return page

    def store_words(self, base_key, words):
        page = self.get_current_key_page(base_key)
        i = 0
        for word in words:
            stored = False
            if word:
                while not stored:
                    print 'storing: %s:%s (%s, %d)' % (base_key, page, word, i)
                    try:
                        a = {word : str(i)}
                        self.domain.put_attributes('%s:%s' % (base_key, page),
                                                   a, False)
                        i += 1
                        stored = True
                    except EC2ResponseError, e:
                        print 'caught SDB Error'
                        print e
                        if e.status == 409:
                            page = self.create_new_key_page(base_key, page)
                            print 'Page Full: creating page %d' % page
                        else:
                            time.sleep(5)
            else:
                pass

    def tally_words(self, words):
        self.total_words += len(words)
        self.unique_words.update(words)

    def process_files(self):
        files = os.listdir(self.path)
        for file in files:
            t = os.path.splitext(file)
            if t[1] == '.txt':
                base_key = t[0]
                self.store_file(file, base_key)
                msg = self.get_msg(file)
                text = self.get_msg_text(msg)
                words = self.get_words(text)
                self.tally_words(words)
                self.store_words(base_key, words)

    def count_words(self):
        files = os.listdir(self.path)
        for file in files:
            msg = self.get_msg(file)
            text = self.get_msg_text(msg)
            words = self.get_words(text)
            self.tally_words(words)

    def count_items(self):
        num_pages = 0
        num_files = 0
        files = os.listdir(self.path)
        for file in files:
            t = os.path.splitext(file)
            if t[1] == '.txt':
                num_files += 1
                base_key = t[0]
                a = self.domain.get_attributes(base_key)
                num_pages += len(a['page'])
        print 'Total Files: %d' % num_files
        print 'Total Pages: %d' % num_pages

    def storage_report(self):
        num_items = 0
        num_attrs = 0
        total_bytes = 0
        rs = self.domain.query("['page'starts-with'']")
        for item in rs:
            num_items += 1
            total_bytes += len(item) + 45
            pages = self.domain.get_attributes(item)
            for page in pages['page']:
                num_attrs += 1
                total_bytes += len('page') + len(page) + 45
                page_name = '%s:%s' % (item, page)
                num_items += 1
                total_bytes += len(page_name) + 45
                attrs = self.domain.get_attributes(page_name)
                for attr_name in attrs:
                    for value in attrs[attr_name]:
                        num_attrs += 1
                        total_bytes += len(attr_name) + len(value) + 45
        print 'Number Of Items: %d' % num_items
        print 'Number Of Name/Value Pairs: %d' % num_attrs
        print 'Total Storage: %d bytes' % total_bytes
                
def test(domain='sdbindex7'):
    i = Indexer('/Users/mitch/Projects/fulltext/INBOX', domain)
    i.process_files()
    return i

def test1():
    i = Indexer('/Users/mitch/Projects/fulltext/INBOX')
    i.count_words()
    return i
    
