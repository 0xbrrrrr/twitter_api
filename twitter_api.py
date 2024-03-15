"""Twitter Scraping API."""
import json
from typing import Any, Generator

import pandas as pd
import tweepy

# The twitter account user ID
USER_ID = ""
# The maximum number of tweets fetched. Used to control the API quota.
MAX_RESULT_COUNT = 2000
# The file path to save the date.
DATAPATH = "text_tweets.txt"
MENTION_PATH = "mentions.csv"  # mentions CSV output
ANNOTATION_PATH = "annotations.csv"  # annotations CSV output
# Api Credentials
credentials = {
    "consumer_key": "",
    "consumer_secret": "",
    "access_token": "",
    "access_token_secret": "",
    "bearer_token": "",
}


def group_dataframe(
    dataframe: pd.DataFrame, grouping_field: str
) -> pd.DataFrame:
    """Return a grouped dataframe.

    A column "tweet_created_at" and "tweet_id" is required.
    """
    dataframe = (
        dataframe.groupby(grouping_field)
        .agg(
            count=(grouping_field, "count"),
            last_tweeted_at=("tweet_created_at", "max"),
            last_tweet_url=("tweet_id", "max"),
        )
        .sort_values("count", ascending=False)
    )
    dataframe["last_tweet_url"] = dataframe["last_tweet_url"].map(
        lambda tweet_id: f"https://twitter.com/robotventures/status/{tweet_id}"
    )
    return dataframe


def search_tweets(
    user_id: str, result_count: int = 100
) -> Generator[dict[str, Any], None, None]:
    """Yield the tweets results."""
    client = tweepy.Client(**credentials)
    pagination_token = None
    count = 0
    max_results = min(100, result_count)
    pagination_tokens = set()
    while True:
        response = client.get_users_tweets(
            id=user_id,
            max_results=max_results,
            tweet_fields=[
                "entities",
                "context_annotations",
                "conversation_id",
                "created_at",
            ],
            exclude=["replies"],
            pagination_token=pagination_token,
        )
        # Get the next token and add it
        try:
            next_token = response.meta["next_token"]
        except (AttributeError, KeyError):
            print(f"Cannot Extract token: {response}")
            break
        # Yield all data
        try:
            for result in response.data:
                data = result.data
                data["next_token"] = next_token
                yield data
                count += 1
        except (AttributeError, KeyError):
            print(f"Cannot Extract result: {response}")
            break
        # Step One: Get the pagination token

        if next_token == pagination_token:
            print("No more data")
            break
        if next_token in pagination_tokens:
            print("Token already collected")
            break
        pagination_token = next_token
        if pagination_token is None:
            break
        pagination_tokens.add(pagination_token)
        print(f"Pagination Token after {count} result: {pagination_token}")
        if count >= result_count:
            break


def fetch_results(user_id: str, max_result_count: int) -> None:
    """Get the date from the Twitter API."""
    with open(DATAPATH, "a", encoding="utf-8") as file:
        for data in search_tweets(
            user_id=user_id, result_count=max_result_count
        ):
            file.write(json.dumps(data) + "\n")


def process_results() -> None:
    """Read the file and extract results."""
    with open(DATAPATH, encoding="utf-8") as file:
        all_mentions = []
        all_annotations = []
        for row in file.readlines():
            data = json.loads(row)
            entities = data.get("entities")
            # Extract the tweet ID and creation date.
            tweet_created_at = data["created_at"]
            tweet_id = data["id"]
            # Get the annotations from the tweets.
            annotations = entities.get("annotations", [])
            if annotations:
                for annotation in annotations:
                    annotation_type = annotation.get("type")
                    normalized_text = annotation.get("normalized_text", "")
                    all_annotations.append(
                        {
                            "normalized_text": normalized_text,
                            "annotation_type": annotation_type,
                            "tweet_created_at": tweet_created_at,
                            "tweet_id": tweet_id,
                        }
                    )
            # Get the mentions from the tweets.
            mentions = entities.get("mentions", [])
            if mentions:
                for mention in mentions:
                    username = mention.get("username", "")
                    all_mentions.append(
                        {
                            "username": username,
                            "tweet_created_at": tweet_created_at,
                            "tweet_id": tweet_id,
                        }
                    )
        mention_dataframe = pd.DataFrame(all_mentions)
        annotation_dataframe = pd.DataFrame(all_annotations)
        mention_aggregation = group_dataframe(
            dataframe=mention_dataframe, grouping_field="username"
        )
        annotation_aggregation = group_dataframe(
            dataframe=annotation_dataframe, grouping_field="normalized_text"
        )
        mention_aggregation.to_csv(MENTION_PATH)
        annotation_aggregation.to_csv(ANNOTATION_PATH)


def main(fetch_data: bool = False, process_data: bool = False) -> None:
    """Full Processing."""
    if fetch_data:
        fetch_results(user_id=USER_ID, max_result_count=MAX_RESULT_COUNT)
    if process_data:
        process_results()


if __name__ == "__main__":
    main()
