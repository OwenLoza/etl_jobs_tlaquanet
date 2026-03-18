CREATE OR REPLACE TABLE user_sentiment_metrics AS
WITH  user_sentiment AS (
    select author_id, content, SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-7b',
        CONCAT('Respond with exactly "TRUE" if the comment is overall positive, and "FALSE" otherwise: ', content)
    ) as sentiment_metric from posts
)
SELECT * from user_sentiment;