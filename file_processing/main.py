import csv
import re
from datetime import datetime


def format_tags(tags_string):
    temp = re.sub(TAGS_REPLACE_REGEX, ',', tags_string).split(
        ',')
    temp[0] = '"' + temp[0] + '"'
    return '{' + ','.join(temp).replace('"', "'") + '}'


def format_date(date_string, input_pattern, output_pattern):
    return datetime.strptime(date_string, input_pattern).strftime(output_pattern)


def format_timestamp(timestamp_string):
    # sth here


FILE_NAME = 'CAvideos.csv'

# Regex for matching any '|' character outside of the quotes, used for proper tags formating
TAGS_REPLACE_REGEX = re.compile('\|(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)')

with open(FILE_NAME, newline='\n', encoding="utf8") as csvfile:
    file = csv.reader(csvfile, delimiter=',', quotechar='"')
    row_number = 0
    with open('output.csv', 'w', newline='', encoding="utf8") as csvfile:
        outputCsv = csv.writer(csvfile, delimiter=',',
                               quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        for row in file:
            if row_number == 0:
                row_number += 1
                outputCsv.writerow(row)
                continue
            # Formating the date
            row[1] = format_date(row[1], '%y.%d.%m', '%Y-%m-%d')
            # Formating the tags
            row[6] = format_tags(row[6])
            # TODO: Formating the trending date timestamp

            outputCsv.writerow(row)

            row_number += 1
