SELECT
    subreddit,
    author,
    body,
    score,
    created_utc,
    controversiality
FROM "AwsDataCatalog"."reddit"."a05"
WHERE (
        REGEXP_LIKE(LOWER(subreddit), '^(artificial|artificialintelligence|ai|machinelearning|ml|deeplearning)$') OR
        REGEXP_LIKE(LOWER(subreddit), '^(chatgpt|openai|gpt|claude|anthropic|bard)$') OR
        REGEXP_LIKE(LOWER(subreddit), '^(midjourney|stablediffusion|dalle|dalle2|aiart|comfyui)$') OR
        REGEXP_LIKE(LOWER(subreddit), '^(tensorflow|pytorch|keras|huggingface|localllama)$') OR
        REGEXP_LIKE(LOWER(subreddit), '^(singularity|agi|automation|aiethics)$') OR
        REGEXP_LIKE(LOWER(subreddit), '.*artificial.*intelligence.*') OR
        REGEXP_LIKE(LOWER(subreddit), '.*machine.*learning.*') OR
        REGEXP_LIKE(LOWER(subreddit), '.*deep.*learning.*') OR
        REGEXP_LIKE(LOWER(subreddit), '.*generative.*ai.*') OR
        REGEXP_LIKE(LOWER(subreddit), '.*chat.*gpt.*') OR
        REGEXP_LIKE(LOWER(subreddit), '.*stable.*diffusion.*')
    )
    AND LENGTH(body) BETWEEN 50 AND 2000
    AND score >= 1
    AND body NOT IN ('[deleted]', '[removed]')
    AND body IS NOT NULL
    AND author IS NOT NULL
    AND author != 'AutoModerator'
ORDER BY score DESC, created_utc DESC