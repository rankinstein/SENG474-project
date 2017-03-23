from bigquery import get_client
import csv
import sys
import getopt
import math
import re

url_removal = re.compile(r"""\(?(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))\)?""")
bracket_removal = re.compile(r'[\[\]\(\)]')
moderated_removal = re.compile(r'\[deleted\]|\[removed\]')
document_cleaning = re.compile(r'[–:“”-]|\w[\*,\']+\w|\.|\d|\'|\\|\"|\?|/’|~|;|,')
whitespace_condesing = re.compile('\s+')


def process_args(argv):
    try:
        opts, args = getopt.getopt(argv,"htd:s:o:r:l:",["testing", "datafile=","subreddit=","offset=","rows=","limit="])
    except getopt.GetoptError:
        print('test.py -i <inputfile> -o <outputfile>')
        sys.exit(2)
    offset = 0
    limit = 1000
    test_set = False
    for opt, arg in opts:
        if opt == '-h':
            print('query.py --datafile <data.csv> --subreddit <subreddit to query> --offset <integer> --rows <integer> --limit <integer>')
            sys.exit(2)
        elif opt in ('-d', '--datafile'):
            file_name = arg
        elif opt in ('-s', '--subreddit'):
            subreddit = arg
        elif opt in ('-o', '--offset'):
            offset = int(arg)
        elif opt in ('-r', '--rows'):
            total_rows = int(arg)
        elif opt in ('-l', '--limit'):
            limit = int(arg)
        elif opt in ('-t', '--testing'):
            test_set = True
    return (file_name, subreddit, offset, limit, total_rows, test_set)

def get_data(file_name, subreddit, offset, limit, total_rows, test_set = False):
    json_key = 'key.json'

    client = get_client(json_key_file=json_key, readonly=True)

    training_tables = """
        [fh-bigquery:reddit_posts.2015_12],
        [fh-bigquery:reddit_posts.2016_01],
        [fh-bigquery:reddit_posts.2016_02],
        [fh-bigquery:reddit_posts.2016_03],
        [fh-bigquery:reddit_posts.2016_04],
        [fh-bigquery:reddit_posts.2016_05],
        [fh-bigquery:reddit_posts.2016_06],
        [fh-bigquery:reddit_posts.2016_07],
        [fh-bigquery:reddit_posts.2016_08],
        [fh-bigquery:reddit_posts.2016_09],
        [fh-bigquery:reddit_posts.2016_10],
        [fh-bigquery:reddit_posts.2016_11],
        [fh-bigquery:reddit_posts.2016_12],
        [fh-bigquery:reddit_posts.2017_01]
    """

    test_tables = """
        [fh-bigquery:reddit_posts.2017_02]
    """

    tables = test_tables if test_set else training_tables

    data_query = """
    SELECT
        gilded > 0 as gilded,
        title,
        selftext
    FROM
        {3}
    WHERE
        subreddit="{0}"
    LIMIT
        {1}
    OFFSET
        {2}
    """.format(subreddit, limit, offset, tables)
    # print('query: {0}'.format(data_query))
    job_id, _result = client.query(data_query)
    complete, row_count = client.check_job(job_id)
    results = client.get_query_rows(job_id)
    fields = list(map(lambda x: [clean_document(x['title'], x['selftext']), 1 if bool(x['gilded']) else 0], results))
    with open(file_name, 'a') as f:
        writer = csv.writer(f)
        for line in fields:
            writer.writerow(line)

    # for q in queries:
    #     job_id = q[0]
    #     complete, row_count = client.check_job(job_id)
    #     results = client.get_query_rows(job_id)
    #     fields = list(map(lambda x: [1 if bool(x['gilded']) else 0, clean_document(x['title'], x['selftext'])], results))
    #     with open(file_name, 'a') as f:
    #         writer = csv.writer(f)
    #         for line in fields:
    #             writer.writerow(line)

    # Submit an async query.
    #job_id, _results = client.query(data_query)

    # Check if the query has finished running.
    #complete, row_count = client.check_job(job_id)

    # Retrieve the results.
    #results = client.get_query_rows(job_id)
    #print('complete? {0}'.format(complete))
    #print(results)

def clean_document(title, selftext):
    text = '{0} {1}'.format(title, selftext).lower()
    text = url_removal.sub('', text)
    text = moderated_removal.sub('', text)
    text = bracket_removal.sub('', text)
    text = document_cleaning.sub(' ', text)
    text = whitespace_condesing.sub(' ', text)
    return text.strip()

if __name__ == '__main__':
    (file_name, subreddit, offset, limit, total_rows, test_set) = process_args(sys.argv[1:])
    get_data(file_name, subreddit, offset, limit, total_rows, test_set)
