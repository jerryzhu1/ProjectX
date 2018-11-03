import urllib.request
from bs4 import BeautifulSoup
import json
import requests
import re
import pandas as pd
import time
from pymongo import MongoClient

# Scrayping
# data extraction
def html_parsing(line):
    record = []
    title = line.find("a", class_= re.compile('title*')).text
    time_submitted_obj = line.find("time")
    time_sub = time_submitted_obj['datetime']
    author = line.find("a", class_='author').text
    comments = line.find("a", class_= ('bylink'))['href']
    record = [title, time_sub, author, comments]
    return record

# data extraction
def html_parsing_comments(soup_c):
    record = []
    title = soup_c.find("a", class_= ('title')).text
    approve = soup_c.find("div", class_= ('score likes')).text
    comment_number = soup_c.find("a", class_= ('bylink')).text.replace(' comments','')

    fst_cmt_all = soup_c.find("div", class_= ('sitetable nestedlisting'))
    fst_cmt = fst_cmt_all.find("div", class_= ('entry unvoted'))

    top_comment_author = fst_cmt.find("a", class_= ('author')).text
    #top_comment_approve = fst_cmt.find("span", class_= ('score unvoted')).text.replace(' points','')
    top_comment = fst_cmt.find("div", class_= ('md')).text

    #record = [title, approve, comment_number, top_comment_author, top_comment_approve, top_comment]
    record = [title, approve, comment_number, top_comment_author, top_comment]
    return record

# web crawling
url = "https://old.reddit.com/top/"
r = requests.get(url, headers = {'User-agent': 'your bot 0.1'})
soup = BeautifulSoup(r.text, 'html.parser')

table = soup.find("div", id = 'siteTable')
block  = table.findAll("div",attrs={'class':re.compile("thing*")})

records = []
for line in block:
    record = html_parsing(line)
    records.append(record)

# data transformation
df = pd.DataFrame(records, columns=['title', 'time_submitted', 'author', 'comments link'])
df_mongo = df.to_dict('records')

#Database
uri = 'mongodb://Jason<password>@cluster0-shard-00-00-opsao.mongodb.net:27017,cluster0-shard-00-01-opsao.mongodb.net:27017,cluster0-shard-00-02-opsao.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true'
client = MongoClient(uri)
db = client['test']
collection = db['test1']
collection.insert_many(df_mongo)
