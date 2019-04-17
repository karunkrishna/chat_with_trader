import pandas as pd
import os
import time
import re
pd.set_option('display.width',1000, 'display.max_columns',1000, 'display.max_rows',1000)


completed_json_dir = 'downloaded_metadata'

raw_files = []
for _, _, fname in os.walk(completed_json_dir):
    raw_files.extend(fname)

raw_files = [completed_json_dir + '/' + fname for fname in raw_files if '.json' in fname]

print(raw_files)
df = []
for json_data in raw_files:
    data = pd.read_json(json_data, typ='series').to_frame().T
    df.append(data)

def clean_up_notes(note):
    note = re.sub(r'^([\s\S]*?)\* \* \*', '', note, re.I | re.MULTILINE)
    note = re.sub(r'(.*?)SHARES', '', note)
    note = re.sub(r'(.*?)ShareTweetSubscribe', '', note)
    note = re.sub(r'(.*?)Tweet this', '', note, re.IGNORECASE | re.MULTILINE)
    note = note.strip()
    note = re.sub(r'\* \* \*', '', note, re.I)
    note = note.replace('\n\n\n', '\n')
    note = note[:note.rfind('\n')]
    note = note.split('\n\n')
    note = [p.replace('\n', ' ') for p in note]
    note = '\n\n'.join(note)
    note = note.replace('*', '\n* ')
    note = note.split('\n')
    note = [p for p in note if '  ' != p]
    note = '\n'.join(note)
    note = re.sub(r"^\d+[^']", '', note, re.MULTILINE)
    note = re.sub(r"\d+[^']$", '', note, re.MULTILINE)
    note = note.strip()
    return note


df = pd.concat(df)
df['uploaded'] = pd.to_datetime(df['uploaded'], infer_datetime_format=True)
df['duration'] = df['duration'].apply(lambda x: time.strftime('%H:%M:%S', time.gmtime(x)))
df['episode'] = df['title'].apply(lambda x: x.split(':')[0])
df['episode'] = df['episode'].apply(lambda x: re.findall(r'\d+', x)[0].lstrip("0"))
df['title'] = df['title'].apply(lambda x: ''.join(x.split(':')[1:]))
df['notes'] = df['notes'].apply(lambda x: clean_up_notes(x))
df.set_index('episode', inplace=True)
df.drop('article',axis=1, inplace=True)


writer = pd.ExcelWriter('output/processed_data.xlsx')
df.to_excel(writer, 'Episode')
writer.save()
writer.close()


print(df)

