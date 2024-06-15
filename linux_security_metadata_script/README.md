### RUN intro_full_text_metadata.py 
- If you would like to run the script without saving database. Use this command:
```
python intro_full_text_metadata.py 
```

- Use this command if you want to check for specific ID
```
python intro_full_text_metadata.py --id 348482
```

- Use this command if you want to commit the changes into database.
```
python intro_full_text_metadata.py --commit

```

### config.ini  explantion

- `[mysql]` section for database
- `[metadata-01]`
   - `logfile`  The name of log file in which intro_full_text_metadata.py  script will write the log
   - `store_state_file` The name of the state file, this will store the state of the runningscript when the keyboard is     interrupted.
   - `limit` The parameter will decide how many records will come in one SQL query .This parameter has nothing to do with the process number of records.
   -`max_words` This parameter decides how many words from the fulltext are considered for generating keywords and metadescriptions.
   -`max_tokens`  This parameter divides the full text into sub-segments based on tokens and creates multiple asynchronous requests within a single request to process all the full text after max-words.
   -`json_file`  Name of the file where metekeywords and description will be written.

 



