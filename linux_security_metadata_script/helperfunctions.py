import logging
from logging import Logger
from copy import copy
from pandas import DataFrame
import math
import re
import tiktoken
from bs4 import BeautifulSoup
import pandas as pd
import json
import os
tokenizer = tiktoken.get_encoding("cl100k_base")
from pymysql import Connection,MySQLError
class ColoredFormatter(logging.Formatter):
    datefmt = "%Y-%m-%d %H:%M:%S"
    MAPPING = {
        'DEBUG'   : 37, # white
        'INFO'    : 36, # cyan
        'WARNING' : 33, # yellow
        'ERROR'   : 31, # red
        'CRITICAL': 41, # white on red bg
    }
    PREFIX = '\033['
    SUFFIX = '\033[0m'

    def __init__(self, patern):
        logging.Formatter.__init__(self, patern)

    def format(self, record):
        colored_record = copy(record)
        levelname = colored_record.levelname
        seq = ColoredFormatter.MAPPING.get(levelname, 37) # default white
        colored_levelname = ('{0}{1}m{2}{3}') \
            .format(ColoredFormatter.PREFIX, seq, levelname, ColoredFormatter.SUFFIX)
        colored_record.levelname = colored_levelname
        return logging.Formatter.format(self, colored_record)
    

def getlogger(name, level=logging.INFO):
    logger=logging.getLogger(name)
    logger.setLevel(level)

    file=logging.FileHandler(name)
    file.setFormatter(logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(file)

    console=logging.StreamHandler()
    console.setFormatter(ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console)
    return logger


def split_into_many(text: str, max_tokens: int = 100) -> list[str]:
        """
        spliting the text into the given pattern and add the spliting text in a one sentence which split text 
            have token length less than 100
            """
        sentences = re.split("(?<=[.。!?।]) +", text)
        n_tokens = [len(tokenizer.encode(" " + sentence)) for sentence in sentences]
        for i, (sentence, token) in enumerate(zip(sentences, n_tokens)):
            if token > max_tokens:
                extra_sentences = sentence.splitlines(keepends=True)
                extra_tokens = [
                    len(tokenizer.encode(" " + extra_sentence))
                    for extra_sentence in extra_sentences
                ]
                del n_tokens[i]
                del sentences[i]
                n_tokens[i:i] = extra_tokens
                sentences[i:i] = extra_sentences

        chunks = []
        tokens_so_far = 0
        chunk = []
        for i, (sentence, token) in enumerate(zip(sentences, n_tokens)):
            if tokens_so_far + token > max_tokens or (i == (len(n_tokens) - 1)):
                chunks.append(" ".join(chunk))
                chunk = []
                tokens_so_far = 0

            chunk.append(sentence)
            tokens_so_far += token + 1

        return chunks



def aggregate_into_few(df:DataFrame,logger:Logger):

    """
    Aggregate the text into list elements by adding the multiple
      rows into one single list element with 1000 token size
        and if the token exceeds by 1000 then it will add up into the new index of the list
    """
    aggregate_text=[]
    current_text=""
    current_length=0
    max_index = len(df["n_tokens"]) - 1
    token_length = sum(df["n_tokens"])
    if max_index == 0:
        return df["text"]
    if token_length > 1000:
        max_length = round(token_length / math.ceil(token_length / 1000)) + 100
    else:
        max_length = 1000
    for i, row in df.iterrows():
        current_length+=row['n_tokens']
        if current_length>max_length:
            aggregate_text.append(current_text)
            current_length=0
            current_text=""
        current_text+=row['text']

        if max_index==i:
            aggregate_text.append(current_text)
    return aggregate_text


def clean_text(fulltext:str,logger:Logger,max_words:int):
    """Converting the text to a soup and extracting only text from the soup  and  the spliting the text based on the max words 
         and return the text """
    soup=BeautifulSoup(fulltext,'html.parser')
    text=soup.get_text(separator=" ")
    cleaned_string = re.sub(r'\n\s*\n', '\n',text)
    split_text=cleaned_string.split()
    if len(split_text)>max_words:
        cleaned_string=" ".join(split_text[:max_words])
    return cleaned_string





def process_text(text:str,logger:Logger,max_tokens:int):
    """ Apply a tokenization to the text to count the token if the token length is greater then max_token then 
      forward the text to split into many function to split the text into multiple rows with token lenggth 100  """
    tokens = len(tokenizer.encode(text))
    if tokens > max_tokens:
        chunks=split_into_many(text)
        df=pd.DataFrame(chunks,columns=['text'])
        df['n_tokens']=df.text.apply(lambda x : len(tokenizer.encode(x)))
        return df
    else:
        df=pd.DataFrame([text],columns=['text'])
        df['n_tokens']=df.text.apply(lambda x : len(tokenizer.encode(x)))
        return df
        

def write_into_the_json_file(response:dict,json_file:str):
    """Append a record to a JSON file"""
    try:
        with open(json_file,'r+') as file:
            filedata=json.load(file)
            filedata.append(response)
            file.seek(0)
            json.dump(filedata, file, indent=4)
    except FileNotFoundError:
        with open(json_file, 'w') as file:
            json.dump([response], file, indent=4)
    except json.JSONDecodeError:
        with open(json_file, 'w') as file:
            json.dump([response], file, indent=4)


def current_state(index_filename: str, id: int = 0, counter: int = 0, mode='r'):
    """ To store the current state """
    filename = os.fspath(index_filename)
    baseFilename = os.path.abspath(filename)
    if os.path.exists(baseFilename) == False or mode == 'w':
        with open(index_filename, 'w') as f:
            json.dump({
                'id': id,
                'counter': counter
            }, f, indent=4)
        return id, counter
    else:
        with open(baseFilename, 'r') as f:
            data = json.load(f)
            return int(data.get('id')), int(data.get('counter'))



def percentage(number, total):
    per = float(number)/float(total)
    to_str = "{:.1%}".format(per)
    return to_str

def get_path_from_cateories_table(connection,catid):
    sql=f"SELECT c.path FROM `xu5gc_categories` as c WHERE `id`={catid}"
    with connection.cursor() as cursor:
        cursor.execute(sql)
        record=cursor.fetchone()
    path=record.get('path')
    if path=="uncategorised":
        return None
    return path


def get_record(connection,alias):
    sql=f"SELECT c.id,c.title,c.url,c.opengraph,c.twitterCards FROM `xu5gc_easyfrontendseo` as c WHERE `url` LIKE '%{alias}%'"
    with connection.cursor() as cursor:
        cursor.execute(sql)
        record=cursor.fetchone()
    return record

def get_prepare_json(record,description,base_url,image_tag):
    opengarph_json_data = {
                "title": record['title'],
                "description": description,
                "image": image_tag,
                "type": "article",
                "site_name": "Linux Security",
                "url": f"{base_url}/{record['url']}",
                "image:alt": record['title']
            }
    twitter_Cards_json_data = {
        "title": record['title'],
        "description": description,
        "image": image_tag,
        "card":"summary_large_image",
        "site":"lnxsec",
        "creator":"lnxsec",
        "image:alt": record['title']
    }
    opengarph_json_data = json.dumps(opengarph_json_data)
    twitter_Cards_json_data=json.dumps(twitter_Cards_json_data)
    return opengarph_json_data,twitter_Cards_json_data

def get_prepare_json_for_new_entry(title,description,url,base_url,image_tag):
    opengarph_json_data = {
                "title": title,
                "description": description,
                "image": image_tag,
                "type": "article",
                "site_name": "Linux Security",
                "url": f"{base_url}/{url}",
                "image:alt": title
            }
    twitter_Cards_json_data = {
        "title":title,
        "description": description,
        "image": image_tag,
        "card":"summary_large_image",
        "site":"lnxsec",
        "creator":"lnxsec",
        "image:alt": title
    }
    opengarph_json_data = json.dumps(opengarph_json_data)
    twitter_Cards_json_data=json.dumps(twitter_Cards_json_data)
    return opengarph_json_data,twitter_Cards_json_data

def do_update(connection: Connection, alias: str, metadata: list, description: str,content_table_id:int,logger:Logger,base_url:str,content_table_title:str,catid:int,images:str):
    try: 
        if len(description)>150:
            print(f" The description length of {content_table_id} is {len(description)} -- ")
        images=json.loads(images)['image_fulltext'] if images!="" else ""
        image_tag=f"{base_url}/{images}" if images!="" else ""
        if metadata is None and description is None:
            return False
        if len(metadata)>=1:
            metadata=",".join(metadata)
        record=get_record(connection,alias)
        if record:
            id=record['id']
            if record["opengraph"]=="" and record["twitterCards"]=="":
                opengarph_json_data,twitter_Cards_json_data= get_prepare_json(record,description,base_url,image_tag)
                sql= """
                    UPDATE xu5gc_easyfrontendseo
                    SET keywords = %s, description = %s, opengraph = %s, twitterCards = %s
                    WHERE id = %s AND (opengraph IS NULL OR opengraph = '') AND (twitterCards IS NULL OR twitterCards = '')
                    """
                args=(metadata,description,opengarph_json_data,twitter_Cards_json_data,id)
            else:
                sql= """
                    UPDATE xu5gc_easyfrontendseo
                    SET keywords = %s, description = %s, opengraph = %s, twitterCards = %s
                    WHERE id = %s 
                    """
                open_graph,twitter_Cards=json.loads(record["opengraph"]),json.loads(record["twitterCards"])
                open_graph["description"]=description
                twitter_Cards["description"]=description
                open_graph['image']=image_tag
                twitter_Cards['image']=image_tag
                args = (metadata, description,json.dumps(open_graph),json.dumps(twitter_Cards), id)     
        else:
            logger.info(f"""ID:{content_table_id} "title":{content_table_title} "Alias": {alias} Record not found in easyfrontseo table Creating the new entry--""")
            path=get_path_from_cateories_table(connection,catid)
            if path is not None:
                url=f"{path}/{alias}"
            else:
                url=alias
            opengarph_json_data,twitter_Cards_json_data=get_prepare_json_for_new_entry(content_table_title,description,url,base_url,image_tag)
            sql="INSERT INTO xu5gc_easyfrontendseo (url, title, description, keywords, generator,robots, openGraph, twitterCards, canonicalUrl,thumbnail) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            args=(url,content_table_title,description,metadata,"","index, follow",opengarph_json_data,twitter_Cards_json_data,f"{base_url}/{url}","")
     
        with connection.cursor() as cursor:
            cursor.execute(sql, args)
            connection.commit()
            logger.info(f'ID:{content_table_id} "title":{content_table_title} "Alias": {alias} - has been updated in database')
            return True
    except MySQLError as e:
        connection.rollback()
        raise e
    except Exception as e:
        logger.info(json.dumps({"id":content_table_id,"message":str(e)}))