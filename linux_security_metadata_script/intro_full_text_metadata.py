import configparser
import os
from helperfunctions import getlogger, percentage,aggregate_into_few,process_text,clean_text,write_into_the_json_file,current_state,do_update
import pymysql
import pymysql.cursors
from logging import Logger
from typing import Dict, Any,Optional,List
from openai import OpenAI,AsyncOpenAI
import asyncio
import pandas as pd
from pandas import DataFrame
import json
import argparse
import concurrent.futures
from pymysql import Connection
client = OpenAI(api_key=os.getenv("OPENAI_SECRET_KEY"))



def get_db_connection(config,logger):
    """
    established a database connection 
    """
    try:
        # Connect to the database
        connection= pymysql.connect(
            host=config.get("mysql", "host"),
            port=int(config.get("mysql", "port")),
            user=config.get("mysql", "user"),
            password=config.get("mysql", "password"),
            database=config.get("mysql", "database"),
            cursorclass=pymysql.cursors.DictCursor,
        )
        return connection
    except pymysql.MySQLError as e:
        logger.error(f"Get db connection ERROR : {e}")
        raise e
    
def get_total_rows(connection):
    """ To get the length the database records """
    Sql="SELECT count(id) as total FROM xu5gc_content"
    with connection.cursor() as cursor:
        cursor.execute(Sql)
        result=cursor.fetchone()
        return result

def get_limit_rows(connection:pymysql.Connection,limit:int,current_id:int,id:int):
    """ To get the records sequentially based  to limit """
    if id > 0:
        sql = "SELECT c.id, c.introtext, c.fulltext, c.alias ,c.images,c.title, c.catid FROM xu5gc_content AS c WHERE id =%s"
        args = id
    elif current_id>0:
        sql="SELECT c.id , c.introtext , c.fulltext, c.alias, c.images,c.title, c.catid FROM xu5gc_content AS c  WHERE id > %s ORDER BY id LIMIT %s" 
        args=(current_id,limit)
    else:
        sql="SELECT c.id , c.introtext , c.fulltext , c.alias,c.images ,c.title ,c.catid FROM xu5gc_content AS c ORDER BY id LIMIT %s" 
        args=limit
    with connection.cursor() as cursor:
        cursor.execute(sql,args)
        result=cursor.fetchall()
    return result

async def process_text_async(client, context, logger):
    """This function execute whwn contexts length is more than one """
    try:
        response = await client.chat.completions.create(
            model="gpt-3.5-turbo-1106",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": f"Consise the context for further analysis to find the informative insights from the context: {context}"}
            ]
        )
        return response.choices[0].message.content.strip(), response.usage.completion_tokens
    except Exception as e:
        logger.error(f"Async Process Text ERROR: {e}")
        return None, None


def process_context(contexts:list,logger:Logger,temperature=0):
    """ check the length if the contexts make a api call """
    metadata=[]
    n_tokens=[]
    if len(contexts) ==1:
        try:
            response = client.chat.completions.create(
                        model="gpt-3.5-turbo-1106",    
                        temperature=temperature, 
                        response_format={ "type": "json_object" },
                        messages=[
                           {
                                "role": "system",
                                "content": "Act as an SEO specialist and analyze the given context to identify Meta keywords and create a concise Meta description for SEO purposes. The Meta description should be up to 20 Words long. Ensure the Meta keywords are distinct, surrounded by quotes, and relevant to the given context. Provide a structured JSON output with the top 5 to 8 Meta keywords and the Meta description. Emphasize the importance of SEO best practices in generating accurate Meta data for websites."
                            },
                            {
                                "role": "user",
                                "content": f"""Instructions:
1. Analyze the provided text to identify the most relevant Meta keywords for SEO optimization.
2. Extract 5 to 10 high volume Meta keywords from the given context.
3. Create a consise Meta description that summarize the given context and ensure that the Meta descritpion Words length should not exceed the 20. 
4. If the Meta description Words length is greater than 20 then trim the Meta descritpion that is more than 20 Words.
4. Format each Meta keyword in quotes and separate them with commas, ensuring no repetition and high relevance to the context.

5. Ensure that each Meta keyword is distinct from one another.
6. The Meta keywords and Meta description must be directly derived from the provided context, aligning with SEO best practices.


###

Given Context: {contexts[0]}

###

Example Output:{{
"Meta keywords": ["Keyword1", "Keyword2", "Keyword3"],
"Meta description": "A succinct summary that encapsulates the main points of the content, optimized for search engines and not exceeding 160 characters."
}}
###


JSON Output:"""
}
]
            )
            metadata.append(response.choices[0].message.content.strip())
            n_tokens.append(response.usage.completion_tokens)
        except Exception as e:
            logger.error(f"Process Context ERROR : {e}")
    else:
        async def process_texts_async(contexts, logger):
            async with AsyncOpenAI(api_key=os.getenv('OPENAI_SECRET_KEY')) as client:
                tasks = [process_text_async(client, context, logger) for context in contexts]
                return await asyncio.gather(*tasks)

        results = asyncio.run(process_texts_async(contexts, logger))
        for meta, tokens in results:
            metadata.append(meta)
            n_tokens.append(tokens)
  
    return pd.DataFrame(data={'text':metadata,'n_tokens':n_tokens})


def process_df(df:DataFrame,logger:Logger):
    """ Aggredate the data into list elements"""
    contexts = aggregate_into_few(df=df,logger=logger)
    new_df = process_context(contexts=contexts,logger=logger)
    if len(new_df) > 1:
        return process_df(new_df,logger)
    return new_df




def extract_record_text(record:Dict[str,Any],logger:Logger,max_words:int,max_tokens:int):
    """ process single  record  and extract the id introtext and fulltext .."""
    try:
        id=record.get('id')
        introtext=record.get("introtext")
        fulltext=record.get("fulltext")
        if len(fulltext)>0:
            text=clean_text(fulltext=fulltext,logger=logger, max_words=max_words)
            df=process_text(text=text,logger=logger,max_tokens=max_tokens)
            df=process_df(df=df,logger=logger)
            logger.info(f" Successfully processed  the record ID:{id}")
            return  str(df["text"][0]) 
        else:
            logger.info(f"Record ID: {id} fulltext field has  empty string.")
            return None
    except Exception as e:
        logger.error(f"Extract_record_text ERROR : {e}")




def process_records(result:list,logger:Logger, total:int,counter:int,max_words:int, max_tokens:int,json_file:str,store_state_file:str,connection:Connection,commit: bool = False,base_url:str=None):
    """Process the extracting records """
    for record in result:
        counter+=1
        try:
            logger.info(f'{"*"*20} Processing ID: {record.get("id")} {"*"*20} ({counter}/{total} - {percentage(counter, total)})')
            metadata=extract_record_text(record=record,logger=logger,max_words=max_words,max_tokens=max_tokens)
            if metadata is not None:
                dict_response=json.loads(metadata)
                response={"id":record.get('id'),"metadata":dict_response}
                write_into_the_json_file(response=response,json_file=json_file)
                if commit:
                    succeed=do_update(connection=connection,alias=record["alias"],metadata=response["metadata"]["Meta keywords"],description=response['metadata']["Meta description"],content_table_id=record.get("id"),logger=logger,base_url=base_url,content_table_title=record.get('title'),catid=record.get("catid"),images=record.get("images"))
                    if succeed:
                        pass
                        # logger.info(f'ID: {response.get("id")} has been updated in database')
        except KeyboardInterrupt as e:
            current_id = record.get("id")
            current_state(store_state_file, id=current_id, counter=counter, mode="w")
            logger.info(f"State save till Record ID: {record.get('id')}")
            raise e
        except Exception as e:
            logger.error(f"Process records ERROR : {e} Record_id: {record.get('id')}")
    return counter
      


def main(id: Optional[int] = 0,commit: bool = False,):
    '''
    This is the main fucntion that extracts the required data from the config file
    '''
    config = configparser.ConfigParser(interpolation=None)
    config.read(os.path.join(os.path.dirname(__file__), "config.ini"))
    log_file_name=config.get('metadata-01',"log_file")
    store_state_file = config.get("metadata-01", "store_state_file")
    logger=getlogger(name=log_file_name)
    json_file=config.get('metadata-01',"json_file")
    limit:int=config.getint("metadata-01","limit")
    current_id:int=config.getint("metadata-01","current_id")
    counter:int=config.getint("metadata-01","counter")
    max_words:int=config.getint("metadata-01","max_words")
    max_tokens:int=config.getint("metadata-01","max_tokens")
    base_url:str=config.get("metadata-01","base_url")
    connection=get_db_connection(config,logger)
    total_records=get_total_rows(connection).get('total')
    logger.info(f'{"="*20} Total records : {total_records} {"="*20}')
    current_id, counter = current_state(store_state_file, mode="r")
    while True:
        try:
            result=get_limit_rows(connection=connection, limit=limit,current_id=current_id,id=id)
            if len(result)==0:
                logger.info(f'{"="*20} All records have been processed {"="*20}')
                break
            counter=process_records(result=result,logger=logger,total=total_records,counter=counter,max_words=max_words, max_tokens=max_tokens,json_file=json_file,store_state_file=store_state_file,commit=commit,connection=connection,base_url=base_url)
            current_id=result[-1]['id']
            if id > 0:
                logger.info(f'{"="*20} All records have been processed {"="*20}')
                break
        except Exception as e:
            logger.info(f"ERROR : {e}")
            current_id=result[-1]['id']



if __name__ =="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", default=0, type=int, help="Check for specific ID")
    parser.add_argument("--commit", action="store_true", help="Update the database")
    args = parser.parse_args()
    specific_id = args.id
    is_commit=args.commit
    main(id=specific_id,commit=is_commit)
