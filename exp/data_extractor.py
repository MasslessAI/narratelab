'''
Extract subreddit submissions and clean the text
'''

import datetime as dt
from datetime import date, datetime
from loguru import logger
import pandas as pd
from psaw import PushshiftAPI
import time
import os
import redditcleaner
import re
import base64

api = PushshiftAPI()

start_epoch = int(dt.datetime(2019, 1, 1).timestamp())
#end_epoch = int(dt.datetime(2020, 3, 1).timestamp())
end_epoch = int(time.time())
total = 0

SUBREDDIT = 'seo'
DATA_FILE_NAME = 'data/reddit_submission_{}_{}_{}.tsv'.format(
    SUBREDDIT, datetime.fromtimestamp(start_epoch).strftime("%Y_%m_%d"),
    datetime.fromtimestamp(end_epoch).strftime("%Y_%m_%d"))
while True:
    gen = list(
        api.search_submissions(
            after=start_epoch, before=end_epoch,
            title="what|when|where|who|whom|which|whose|why|how|wonder|want|anyone", is_self=True,
            is_original_content=True, subreddit=SUBREDDIT,
            filter=['title', 'selftext', 'author', 'permalink', 'num_comments', 'score', 'total_awards_received',
                    'upvote_ratio'],
            sort='asc', sort_type='created_utc', limit=500))

    if len(gen) == 0:
        break

    def submission_filter(submission):
        if 'title' not in submission:
            return False
        if 'selftext' not in submission:
            return False
        if 'author' not in submission:
            return False
        if submission['author'] == "[deleted]":
            return False
        if any(submission['selftext'] == x for x in ["[removed]", "[deleted]"]):
            return False
        return True

    def prepare_data(data):
        # some of the fields may be missing
        # must manually set an init value to avoid
        # generating invalid csv
        _data = {
            'title': '',
            'selftext': '',
            'author': '',
            'permalink': '',
            'num_comments': 0,
            'score': 0,
            'total_awards_received': 0,
            'upvote_ratio': 1.0,
            'created_utc': None
        }

        for key in _data:
            if key in data and data[key] is not None:
                _data[key] = data[key]

        return _data


    items = map(prepare_data, [item.d_ for item in gen])

    items = list(filter(submission_filter, items))

    df = pd.DataFrame(items)

    # clean data

    def clean(text):
        # remove reddit styles
        text = redditcleaner.clean(
            text, quote=False, bullet_point=False, link=False, strikethrough=False, spoiler=False, code=False,
            superscript=False, table=False)

        # refer to https://towardsdatascience.com/cleaning-text-data-with-python-b69b47b97b76
        # Remove unicode characters
        text = text.encode('ascii', 'ignore').decode()

        # Remove Hashtags
        text = re.sub("#\S+", " ", text)

        # Remove markdown links
        text = re.sub(r"\[(.+)\]\(.+\)", r"\1", text)

        # Remove other urls
        text = re.sub(r"http\S+", " ", text)

        # remove text inside brackets
        text = re.sub("\(.*?\)"," ", text)
        text = re.sub("\[.*?\]"," ", text)

        # remove quotes
        # remove brackets
        # remove semicolon
        text = re.sub(r'[\t()[\]\"*:\\]',' ', text)

        # remove non-ascii chars
        text = re.sub(r"[^\x00-\x7F]+",' ', text)

         # Replace the over spaces
        text = re.sub('\s{2,}', " ", text)

        return text

    df['title'] = df['title'].map(clean)
    df['selftext'] = df['selftext'].map(clean)

    if not os.path.isfile(DATA_FILE_NAME):
        df.to_csv(DATA_FILE_NAME, sep='\t', header='column_names', index=False, quoting=3)
    else:  # else it exists so append without writing the header
        df.to_csv(DATA_FILE_NAME, sep='\t', mode='a', header=False, index=False, quoting=3)

    start_epoch = items[-1]['created_utc']
    total += len(items)
    logger.info('Added {} Total {} Last created_utc {}'.format(
        len(items), total, date.fromtimestamp(start_epoch)))

    time.sleep(3)
