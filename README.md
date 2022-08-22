# kafka-denormalization

This is a sample project to denormalize two Kafka topics into one. In other words,
it performs a one-to-many join between two topics based on a foreign key, and emits
the joined data to a third topic.

The basic use-case is for when your data is already being produced on some topics, and you need
to combine them over time as updates come in (and updating existing records from either side).

## Example

This repository contains an example using Hacker News comments and stories. The `services` directory
contains 2 microservices, one that polls for new stories and produces them on a topic, and the other for comments.

`hn.comments` (left)
```json
{"by":"zinekeller","id":32546427,"parent":32546388,"text":"...","time":1661132891,"type":"comment","story":32545513}
```

`hn.stories` (right)
```json
{"by":"thesuperbigfrog","descendants":40,"id":32545513,"score":50,"time":1661124181,"title":"The Google Pixel 6a highlights everything wrong with the U.S. phone market","type":"story","url":"https://www.xda-developers.com/google-pixel-6a-us-market-editorial/"}
```

Our objective is to join these 2 topics into one. Each message will contain the comment object, as well as the inflated story object.

`hn.comments-with-story`
```json
{
    "comment": {"by":"zinekeller","id":32546427,"parent":32546388,"text":"...","time":1661132891,"type":"comment","story":32545513},
    "story": {"by":"thesuperbigfrog","descendants":40,"id":32545513,"score":50,"time":1661124181,"title":"The Google Pixel 6a highlights everything wrong with the U.S. phone market","type":"story","url":"https://www.xda-developers.com/google-pixel-6a-us-market-editorial/"}
}
```
