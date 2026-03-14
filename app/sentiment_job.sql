CREATE OR REPLACE TABLE user_sentiment_metrics AS
WITH  user_sentiment AS (
    select author_id, content, SNOWFLAKE.CORTEX.COMPLETE(
    'openai-gpt-4.1',
    CONCAT('TRUE IF COMMENT IS POSITIVE ELSE FALSE', content),
    SNOWFLAKE.CORTEX.COMPLETE(
    'mistral',
    CONCAT('TRUE IF COMMENT IS POSITIVE ELSE FALSE', content)
) as sentiment_metric from posts;
)
SELECT * from user_sentiment;