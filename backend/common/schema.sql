-- postgres
CREATE SCHEMA twitter;

CREATE TABLE twitter.tweets (
	tweet json
);

CREATE TABLE twitter.hashtags (
	tweet_id bigint,
	tweet_timestamp timestamp,
	hashtag text
);