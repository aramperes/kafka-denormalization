from kafka import KafkaProducer
import requests
import json
import time

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

topic = config['kafka']['stories-topic']
producer = KafkaProducer(
    bootstrap_servers=config['kafka']['bootstrap-servers'],
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda m: json.dumps(m).encode('utf-8'))

# a in-memory cache to prevent spamming the same stories
# on startup, the last 500 stories are resent, so the topic will still have dupes
seen_stories = set()


def inflate_story(id):
    print('inflating...', id)
    return requests.get(f'https://hacker-news.firebaseio.com/v0/item/{id}.json').json()


while True:
    print('Fetching new stories...')
    stories = requests.get(
        'https://hacker-news.firebaseio.com/v0/newstories.json').json()
    new_stories = 0

    for story_id in stories:
        if story_id in seen_stories:
            continue

        story = inflate_story(story_id)
        if story is None:
            continue

        producer.send(topic, story, str(story_id))
        seen_stories.add(story_id)
        new_stories += 1

    print('Produced', new_stories, 'new stories')
    time.sleep(15)
