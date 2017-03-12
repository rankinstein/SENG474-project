from bigquery import get_client

# JSON key provided by Google
json_key = 'key.json'

client = get_client(json_key_file=json_key, readonly=True)

# Submit an async query.
job_id, _results = client.query("""
SELECT
  body,
  name,
  author,
  downs,
  ups,
  score,
  gilded,
  controversiality,
  subreddit
FROM
  [fh-bigquery:reddit_comments.2016_08]
LIMIT
  100
""")

# Check if the query has finished running.
complete, row_count = client.check_job(job_id)

# Retrieve the results.
results = client.get_query_rows(job_id)
print(results)
