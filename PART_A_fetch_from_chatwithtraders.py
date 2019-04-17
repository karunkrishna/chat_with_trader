from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import re
from datetime import datetime
from threading import Thread
import pandas as pd
from selenium.common.exceptions import TimeoutException
pd.set_option('display.width',1000, 'display.max_columns',1000, 'display.max_rows',1000)
import html2text
import json
import os

'''
NOTE: This is a two fold manual process because I don't want to troubleshoot
chromdriver for an adhoc request

First, get a fresh list of chat with trader episodes
create export pickles for each episode
Keep manually rerunning until the list is exhausted

'''

chrome_options = Options()
chrome_options.add_argument("--disable-infobars")
chrome_options.add_argument("--disable-notifications")
chromedriver_path = r'C:\executables\chromedriver.exe'


def get_episode_data(url):
    try:
        chrome = webdriver.Chrome(executable_path=chromedriver_path, chrome_options=chrome_options)
        chrome.get(url)
    except TimeoutException as ex:
        print('Timeout', ex.Message)
        chrome.navigate().refresh()

    _page_source = chrome.page_source

    pattern_title = r'class="entry-title">(.*?)</'
    title = re.search(pattern_title, _page_source).group(1)
    print('-   Got Title')

    uploaded_date = chrome.find_element_by_class_name('entry-date').get_attribute('datetime')
    uploaded_date = datetime.strptime(uploaded_date[0:18], '%Y-%m-%dT%H:%M:%S')
    print('-   Got Uploaded date')

    article_raw = chrome.find_element_by_tag_name('article')
    article_raw = article_raw.get_attribute('innerHTML')
    article = html2text.html2text(article_raw)
    print('-   Got Article')

    htmlp = html2text.HTML2Text()
    htmlp.ignore_images = True
    htmlp.ignore_links = True
    notes = htmlp.handle(article_raw)
    print('-   Got Notes')

    iframe_elem = chrome.find_elements_by_tag_name('iframe')[0]
    chrome.switch_to.frame(iframe_elem)
    duration = chrome.find_elements_by_class_name('static-duration')[0].text
    duration = datetime.strptime(duration.replace(' ', ''), '/%H:%M:%S')
    duration = duration - duration.replace(hour=0, minute=0, second=0, microsecond=0)
    print('-   Got Duration')

    record = {'title':title, 'uploaded':uploaded_date.isoformat(), 'duration':duration.total_seconds(), 'article':article,'notes':notes}
    print(record)

    json_export = json.dumps(record, indent=4)
    ep_title = [ep_title for ep_title in url.split('/') if 'ep-' in ep_title][0]
    with open(f'downloaded_metadata/{ep_title}.json', 'w') as f:
        f.write(json_export)

    chrome.quit()


if __name__ == '__main__':
    starttime = datetime.now()

    writer = pd.ExcelWriter('input/episode_url_list.xlsx')

    ## TODO: Step A: Get the latest url for chat with traders, comment out once done
    print('Retrieving full episode list from chat with traders website')
    browser = webdriver.Chrome(executable_path=chromedriver_path, chrome_options=chrome_options)
    browser.get('https://chatwithtraders.com/podcast-episodes/')

    episode_hrefs = browser.find_elements_by_tag_name('a')
    episode_hrefs = [href.get_attribute('href') for href in episode_hrefs]
    episode_hrefs = set(episode_hrefs)
    episode_hrefs = sorted(episode_hrefs)
    episode_count = len(episode_hrefs)
    episode_hrefs = [href for href in episode_hrefs if 'ep-' in href]

    browser.quit()

    latest_list_df = pd.Series(episode_hrefs)
    latest_list_df.to_excel(writer, 'list')

    writer.save()
    writer.close()


    # TODO: Step B: Load the list of completed episodes from downloaded_metadata folder
    episode_hrefs = pd.read_excel('input/episode_url_list.xlsx')
    episode_hrefs.rename(columns={0:'url'}, inplace=True)
    episode_hrefs['key'] = episode_hrefs['url'].apply(lambda x: [ep_title for ep_title in x.split('/') if 'ep-' in ep_title][0])
    episode_hrefs = episode_hrefs[['url','key']]

    completed_json = []
    for root, dir, filename in os.walk('downloaded_metadata'):
        completed_json.extend(filename)

    try:
        completed_json = [fname for fname in completed_json if '.json' in fname]
        completed_json_df = pd.DataFrame(completed_json).rename(columns={0:'key'})
        completed_json_df['key'] = completed_json_df['key'].apply(lambda x: x.replace('.json',''))
        completed_json_df['isdownloaded'] = True
    except:
        pass

    try:
        episode_hrefs = pd.merge(episode_hrefs, completed_json_df, on='key', how='left')
    except:
        episode_hrefs['isdownloaded'] = False

    episode_hrefs.sort_values('key', inplace=True, ascending=True)
    episode_hrefs = episode_hrefs.reset_index().drop('index', axis=1)
    print(episode_hrefs)

    episode_hrefs = episode_hrefs[~(episode_hrefs['isdownloaded'] == True)]
    episode_hrefs = episode_hrefs['url'].to_list()


    print(f'\nRemaining episodes count {len(episode_hrefs)}')

    try:
        for idx, episode in enumerate(episode_hrefs, start=1):
            print(f'Feting web {idx}/{len(episode_hrefs)}  {datetime.now() - starttime} {episode}')
            try:
                get_episode_data(url=episode)
            except Exception as e:
                print(e)
                print('-   Not a valid episode url or selenium has timed out...')

    except Exception as e:
        print(e)
        print('Please run again....')
    else:
        print('Completed. Done')


