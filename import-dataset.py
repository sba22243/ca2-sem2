# read the datasets and filter only the tweets that contains my keywords
# keywords are: covid, vaccine

import json, os, bz2

dataset_path = 'c:\\dataset\\archive'

keywords = ['covid', 'vaccine']

for subdir, _, files in os.walk(dataset_path):
    for file in files:
        if file.endswith('.bz2'):
            filename_bz2 = os.path.join(subdir, file)
            with bz2.open(filename_bz2, 'rb') as f:
                json_content = json.loads(f.read())
                tweets_selected = []
                for tweet in json_content:
                    if any(keyword in tweet['text'] for keyword in keywords):
                        tweets_selected.append(tweet)
                filename = filename_bz2 + ".json"
                with open(filename, 'w') as destination_file:
                    json.dump(tweets_selected, destination_file)
