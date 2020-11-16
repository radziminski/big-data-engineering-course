import csv
import re
import sys
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
    print('not implemented')


def process_file(language):
    ENCODING = 'utf8'
    with open(f'./{INPUT_FOLDER}/{language}videos.csv', newline='\n', encoding=ENCODING) as csvfile:
        file = csv.reader(csvfile, delimiter=',', quotechar='"')
        row_number = 0
        empty_cells = 0
        with open(f'./{OUTPUT_FOLDER}/{language}formatted.csv', 'w', newline='', encoding=ENCODING) as csvfile:
            outputCsv = csv.writer(csvfile, delimiter=',',
                                   quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            try:
                for row in file:
                    try:
                        if row_number == 0:
                            row.append('region')
                            row_number += 1
                            outputCsv.writerow(row)
                            continue
                        # Formating the date
                        row[1] = format_date(row[1], '%y.%d.%m', '%Y-%m-%d')
                        # Formating the tags
                        row[6] = format_tags(row[6])
                        # TODO: Formating the trending date timestamp

                        # Add region to row
                        row.append(language)

                        wrong_cell = False
                        for cell in row:
                            if not cell:
                                wrong_cell = True

                        if wrong_cell:
                            continue

                        outputCsv.writerow(row)

                        row_number += 1
                    except:
                        continue
                print('processed')
            except:
                print(sys.exc_info()[0])


FILE_NAME = 'CAvideos.csv'

# Regex for matching any '|' character outside of the quotes, used for proper tags formating
TAGS_REPLACE_REGEX = re.compile('\|(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)')

REGION = 'CA'
OUTPUT_FOLDER = 'output'
INPUT_FOLDER = 'data'


languages = ['CA', 'DE', 'FR', 'GB', 'IN', 'JP', 'KR', 'MX', 'RU', 'US']
for language in languages:
    process_file(language)
