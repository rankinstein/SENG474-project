import json

data = []
with open('data1.txt') as f:
    for line in f:
        try:
            data.append(json.loads(line))
        except ValueError as e:
            print('line {0} is not a json. ValueError: {1}'.format(line, e))

#print(data)

def data_point_extraction(line):
    return (line['body'], line['ups'], line['downs'], line['subreddit'], line['score'])

d = map(data_point_extraction, data)

print(d)
