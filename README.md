# Introduction
This project analyses reddit comments. The project is implemented in Python (currently using python 3 but should also be compatible with 2.7.x)

# Dataset
Discovered from this post: [](https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/)

The dataset is publicly available using Google's BigQuery service. The collection of all comments since 2015-01-01 is 39.7 GB.

BigQuery requests require an API key. The dataset in BigQuery is [fh-bigquery:reddit_comments.all_starting_201501].

BigQuery has a web interface available at: [](https://bigquery.cloud.google.com/results/seng474-project:bquijob_3a8481a1_15ac45883a2?pli=1). Note that this link may not be accessible without invitation to the seng474-project (Contact jonah@uvic.ca if there are access issues).

# Objective
The exact objective has yet to be determined but could be:
- Determining whether a post has been gilded (posted has received gold).
- Determining whether the score of a post is over a certain value.
- Determining some other aspect of a post such as controversiality etc.

# Approach
The exact approach has yet to be determines but could include:
- Naive Bayes text analysis
- Decision tree based on other metrics such as body length, score, or being gilded

# Data collection

# Pre-processing

# Analysis

# Validation
