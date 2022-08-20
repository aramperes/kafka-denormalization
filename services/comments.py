from kafka import KafkaProducer
import requests
import json
import time
from bs4 import BeautifulSoup

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

topic = config['kafka']['comments-topic']
producer = KafkaProducer(
    bootstrap_servers=config['kafka']['bootstrap-servers'],
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda m: json.dumps(m).encode('utf-8'))

# a in-memory cache to prevent spamming the same comments
# on startup, the last 30 stories are resent, so the topic may have dupes
seen_comments = set()


def comment_story_pairs(html: str):
    # use bs4 to create a list of (comment, story) tuples
    pairs = []

    soup = BeautifulSoup(html, 'html.parser')
    things = soup.find_all('tr', {'class': 'athing'})

    for thing in things:
        comment_id = int(thing['id'])
        on_story = thing.select('.onstory > a')[0]
        story_id = int(on_story['href'].split('=')[-1])
        pairs.append((comment_id, story_id))

    return pairs


def inflate_comment(id, story):
    print('inflating...', id)
    comment = requests.get(f'https://hacker-news.firebaseio.com/v0/item/{id}.json').json()
    if comment is not None:
        comment['story'] = int(story)
    return comment


def partition(id):
    return int(id) % 4


while True:
    print('Fetching new comments...')
    html = requests.get(
        'https://news.ycombinator.com/newcomments').text
    new_comments = 0

    for (comment_id, story_id) in comment_story_pairs(html):
        if comment_id in seen_comments:
            continue

        comment = inflate_comment(comment_id, story_id)
        if comment is None:
            continue

        producer.send(topic, comment, str(comment_id), partition=partition(story_id))
        seen_comments.add(comment_id)
        new_comments += 1

    print('Produced', new_comments, 'new comments')
    time.sleep(15)
