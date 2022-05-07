#!/usr/bin/python3
import os
import copy
import sqlite3
import time
from datetime import datetime, timedelta
from calendar import monthrange
import argparse
import subprocess
import json
import yaml
import uuid
import re
from rich.console import Console
from rich.table import Table 

# log sql table schema
# | task | priority | status | issue | note | created | updated | scheduled

# options
# -a to add entries
# -e [issue_id(s)] to edit one or more entries
# -l [filter] to list entries for the timefilter range (default to today), also accept regex filter on other columns (=/regexp)
# -d [issue_id(s)] to delete entry
# -m to create meeting notes for today
# -f output format (json/csv etc)

BIN_DIR = os.path.dirname(__file__)

TMP_DIR = os.path.join(BIN_DIR, '../.tmp_logs')
DB_PATH = os.path.join(BIN_DIR, '../.simple_note.db')

LOG_TEMP =  {
    'id': '',
    'task': '',
    'priority': 0,
    'status': 'todo',
    'issue': '',
    'created': None,
    'updated': None,
    'scheduled': None,
    'note': ''
}

TIME_FIELD_KEYS = ['created', 'updated', 'scheduled']
STR_FIELD_KEYS = ['id', 'task', 'status', 'issue', 'note']
INT_FIELD_KEYS = ['priority']
READ_ONLY_KEYS = ['id', 'created', 'updated']
EDITABLE_KEYS = ['task', 'priority', 'status', 'issue', 'scheduled', 'note']


def str_presenter(dumper, data):
    """configures yaml for dumping multiline strings
    Ref: https://stackoverflow.com/questions/8640959/how-can-i-control-what-scalar-form-pyyaml-uses-for-my-data"""
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)

yaml.add_representer(str, str_presenter)
yaml.representer.SafeRepresenter.add_representer(str, str_presenter) # to use with safe_dum


def simple_note():
    parser = argparse.ArgumentParser(description='Note optons')
    parser.add_argument('-a', '--add', action='store_true', help='Add new note entries')
    parser.add_argument('-d', '--delete', default=None, help='Specify the log id to be deleted')
    parser.add_argument('-e', '--edit', action='store_true', help='Edit existing note entries')
    parser.add_argument('-l', '--list', action='store_true', help='List note entries')
    parser.add_argument('-v', '--verbose', default=2, type=int, help='Verbose level')
    parser.add_argument('-f', '--filter', help='Apply filter query')
    parser.add_argument('-o', '--output', default='table', help='Choose output format')
    args = parser.parse_args()

    # if the tmp directory is not present, create it
    if not os.path.isdir(TMP_DIR):
        os.mkdir(TMP_DIR)

    conn = create_connection()

    # check if the worklog table exist 
    check_log_table(conn)

    # default filter is scheduled today
    # make a special case for standup meetings
    # if no filter is applied, set the filter to last 2 days
    # or if a Monday, last 4 days
    # (this is a hacky way to do it)
    if args.output == 'standup' and not args.filter:
        if not datetime.now().weekday():
          args.filter='scheduled: range= start_of_today - 3d to now'
        else:
          args.filter='scheduled: range= start_of_today - 1d to now'
    elif not args.filter:
        args.filter='scheduled: range= start_of_today to end_of_today'

    if args.add:
        add_log(conn)

    if args.delete:
        delete_log(args.delete, conn)

    if args.list:
        get_log(
            conn,
            filter=args.filter if 'filter' in args else None,
            output=args.output if 'output' in args else None,
            verbose=args.verbose if 'verbose' in args else None,
            print=True,
        )

    if args.edit:
        edit_log(
            conn,
            filter=args.filter if 'filter' in args else None,
        )

    conn.close()


def create_connection():
    conn = None
    # conn to sqlite3 db
    try:
      conn = sqlite3.connect(DB_PATH)
      return conn
    except sqlite3.Error as e:
        print(e)
        if conn:
            conn.close()
        exit(1)


def check_log_table(conn):
    sql = """SELECT name FROM sqlite_schema WHERE type='table' AND name='WORKLOG';"""
    try:
        cur = conn.cursor()
        result = cur.execute(sql).fetchall()
    except sqlite3.Error as e:
        # this is for backward compatibility
        if str(e) == 'no such table: sqlite_schema':
            sql = sql.replace('schema', 'master')
            result = cur.execute(sql).fetchall()
        else:
            print(e)
            exit(1)
    finally:
        if cur:
            cur.close()

    if not len(result):
        print('WORKLOG table not found; create table')
        sql_create = """
            CREATE TABLE WORKLOG(
                ID NVARCHAR(32) NOT NULL,
                TASK NVARCHAR(255) NOT NULL,
                PRIORITY TINYINT,
                STATUS VARCHAR(25),
                ISSUE VARCHAR(25),
                CREATED INT NOT NULL,
                UPDATED INT NOT NULL,
                SCHEDULED INT,
                NOTE TEXT(1000)
            )
        """
        try: 
            cur = conn.cursor()
            cur.execute(sql_create)
        except sqlite3.Error as e:
            print(e)
            exit(1)
        finally:
            if cur:
                cur.close()
    else: 
        return


def get_log(conn, **kwargs):
    filter = kwargs['filter']
    filter_parsed = parse_filter(filter) 
    fields = ['id', 'task', 'issue', 'priority', 'status', 'scheduled', 'created', 'updated', 'note']

    verbose = kwargs['verbose']

    if verbose <= 1:
        display_fields = [fields[1], fields[3], fields[4]]
    elif verbose == 2:
        display_fields = fields[1:6]
    elif verbose == 3:
        display_fields = fields[1:8]
    else:
        display_fields = fields

    sql_get = f"SELECT {','.join(display_fields)} FROM WORKLOG {filter_parsed}"

    try:
        conn.row_factory = dict_factory 
        cur = conn.cursor()
        result = cur.execute(sql_get).fetchall()

        # print the result if specified otherwise return result object
        if 'print' in kwargs and kwargs['print']:
            output_format = kwargs['output'] if 'output' in kwargs and kwargs['output'] else 'table'
            display_log(result, output_format, filter)
        else:
            return result
    except sqlite3.Error as e:
        print(e)
        exit(1)
    finally:
        conn.row_factory = None
        if cur:
            cur.close()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def display_log(result, output_format, filter):
    if not result:
        print('no log entry found.')
        return

    elif output_format == 'json':
        print(json.dumps(result))

    elif output_format == 'yaml':
        print(yaml.dump(result, allow_unicode=True, sort_keys=False))

    elif output_format == 'csv':
        output = []
        output.append(','.join(result[0].keys()))
        for item in result:
            output.append(','.join([str(i) for i in item.values()]))
        print('\n'.join(output))

    elif output_format == 'table': 
        table = Table(title=f'Tasks table filtered by {filter}')

        for col in result[0].keys():
            table.add_column(col.title())

        for item in result:
            row = []
            for key in item:
                if key.lower() in TIME_FIELD_KEYS:
                    item[key] = datetime.fromtimestamp(item[key]).strftime('%Y-%m-%d %H:%M:%S')
                elif key.lower() == 'note' and len(item[key]) >= 10:
                    item[key] = item[key][0:10] + '...'
                row.append(str(item[key]))
            table.add_row(*row)
        
        console = Console()
        console.print(table)

    elif output_format == 'vi':
        editor = os.environ.get('EDITOR', 'vim')
        log_name = f'{TMP_DIR}/tmp_display_{int(time.time())}'

        for idx,item in enumerate(result):
            for key in item:
                if key.lower() in TIME_FIELD_KEYS:
                    item[key] = datetime.fromtimestamp(item[key]).strftime('%Y-%m-%d %H:%M:%S')

        with open(log_name, 'w+') as tmp:
            template = copy.deepcopy(LOG_TEMP)
            for item in result:
                tmp.write(yaml.dump([item], allow_unicode=True, sort_keys=False))
                tmp.write('\n')
            tmp.flush()
            subprocess.call([editor, '-R', tmp.name])

        # the tmp log can be safely removed here
        try: 
            os.remove(log_name)
        except Exception as e:
            print(e)

    elif output_format == 'standup': 
        entry_by_date = {}
        for item in result:
            row = []
            date_key = None
            for key in item:
                if key.lower() == 'scheduled':
                    s_date = datetime.fromtimestamp(item[key])
                    date_key = s_date.strftime('%Y-%m-%d %A')
                elif key.lower() in TIME_FIELD_KEYS:
                    item[key] = datetime.fromtimestamp(item[key]).strftime('%Y-%m-%d %H:%M:%S')
                    row.append(str(item[key]))
                elif key.lower() == 'note' and len(item[key]) >= 10:
                    item[key] = item[key][0:10] + '...'
                    row.append(str(item[key]))
                else:
                    row.append(str(item[key]))

            entry = ' | '.join(row)

            if date_key:
                if date_key not in entry_by_date:
                    entry_by_date[date_key] = [entry]
                else:
                    entry_by_date[date_key].append(entry)

        today = datetime.now().strftime("%Y-%m-%d %A")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %A")
        print(f'## {datetime.now().strftime("%Y-%m-%d %A")}:')
        new_line = '\n' # to get around f-string not allowing back slash
        space = ' '
        for key in entry_by_date:
            if key == yesterday:
                date_stamp = 'Yesterday'
            elif key == today:
                date_stamp = 'Today'
            else:
                date_stamp = key
            print(f'{new_line}{space*4}{date_stamp}:{new_line}{space*8}{f"{new_line}{space*8}".join(entry_by_date[key])}')


def add_log(conn):
    editor = os.environ.get('EDITOR', 'vim')
    log_name = f'{TMP_DIR}/tmp_add_{int(time.time())}.yaml'
    now = datetime.now()
    start_of_work_hour = datetime(
        year = now.year,
        month = now.month,
        day = now.day,
        hour = 9,
        minute = 30,
        second = 0,
    )

    with open(log_name, 'w+') as tmp:
        template = copy.deepcopy(LOG_TEMP)
        [template.pop(k) for k in READ_ONLY_KEYS]
        template["scheduled"] = start_of_work_hour.strftime('%Y-%m-%d %H:%M:%S')
        text = yaml.dump([template], allow_unicode=True, sort_keys=False).replace('note: \'\'', 'note: |')
        tmp.write(text)
        tmp.flush()
        subprocess.call([editor, tmp.name])

    with open(log_name, 'r') as tmp_r:
        new_log = tmp_r.read()

    # the tmp log can be safely removed here
    try: 
        new_entries = yaml.safe_load(new_log)
        os.remove(log_name)
    except Exception as e:
        print(e)

    # new entry must have either task or issue filled
    new_entries = list(filter(lambda entry: any([entry['task'], entry['issue']]), new_entries))

    # assign unique id and assign timestamp to log entry
    for entry in new_entries:
        entry['id'] = str(uuid.uuid4())[:32]
        entry['created'] = int(time.time())
        entry['updated'] = int(time.time())
        # strip trailing new lines
        entry['note'] = entry['note'].rstrip()
        try:
            entry['scheduled'] = int(datetime.strptime(entry['scheduled'], '%Y-%m-%d %H:%M:%S').timestamp())
        except Exception as e:
            print(e)
            print(f'Schedule {entry["task"]} to start of today')
            entry['scheduled'] = int(start_of_work_hour.timestamp())

    # insert log entry
    records = [tuple([item[k] for k in LOG_TEMP.keys()]) for item in new_entries]
    print(f'{len(records)} record(s) added to worklog.')
    try:
        cur = conn.cursor()
        sql_insert = 'INSERT INTO WORKLOG VALUES(?,?,?,?,?,?,?,?,?);'
        cur.executemany(sql_insert, records)
        conn.commit()
    except sqlite3.Error as e:
        print(e)
        exit(1)
    finally:
        if cur:
            cur.close()
            

def delete_log(id, conn):
    # test for id length
    if len(id) != 32:
        print("Given log id {id} isn't the correct format")
        exit(1)

    cur = conn.cursor()
    sql_delete = f"DELETE FROM WORKLOG WHERE ID = '{id}'"
    try:
        cur.execute(sql_delete)
        conn.commit()
        print(f'Successfully deleted log id: {id}')
    except sqlite3.Error as e:
        print(e)
        exit(1)
    finally:
        if cur:
            cur.close()


def edit_log(conn, **kwargs):
    kwargs['verbose'] = 5
    result = get_log(conn, **kwargs)
    fields = EDITABLE_KEYS
    fields.append('id')
    if not result:
        print('no log entry found.')
        return
    else:
        editor = os.environ.get('EDITOR', 'vim')
        log_name = f'{TMP_DIR}/tmp_edit_{int(time.time())}'
        edit_entries = []

        for idx,item in enumerate(result):
            editable_content = {}
            for key in item:
                if key.lower() not in fields:
                    continue
                elif key.lower() in TIME_FIELD_KEYS:
                    editable_content[key] = datetime.fromtimestamp(item[key]).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    editable_content[key] = item[key]
            edit_entries.append(editable_content)

        with open(log_name, 'w+') as tmp:
            template = copy.deepcopy(LOG_TEMP)
            for item in edit_entries:
                tmp.write(yaml.dump([item], allow_unicode=True, sort_keys=False))
                tmp.write('\n')
            tmp.flush()
            subprocess.call([editor, tmp.name])

        with open(log_name, 'r') as tmp_r:
            updated_log = tmp_r.read()

        # the tmp log can be safely removed here
        try: 
            update_entries = yaml.safe_load(updated_log)
            os.remove(log_name)
        except Exception as e:
            print(e)

        # check of modified log entries
        modified_entries = []
        for idx,entry in enumerate(update_entries):
            if entry != edit_entries[idx] and entry['ID'] == edit_entries[idx]['ID']:
                modified_entries.append(entry)

        # updated log with new fields info, update the updated timestamp then update db
        for entry in modified_entries:
            update_fields = [f' UPDATED = {int(datetime.now().timestamp())}']
            for k in EDITABLE_KEYS:
                if k == 'id':
                    continue
                elif k in TIME_FIELD_KEYS:
                    entry[k.upper()] = int(datetime.strptime(entry[k.upper()], '%Y-%m-%d %H:%M:%S').timestamp())
                elif k in STR_FIELD_KEYS:
                    entry[k.upper()] = "'" + entry[k.upper()] + "'"

                update_fields.append(f' {k.upper()} = {entry[k.upper()]}')

            sql_update = 'UPDATE WORKLOG SET ' + ','.join(update_fields) +  f" WHERE ID = '{entry['ID']}'"

            try:
                cur = conn.cursor()
                cur.execute(sql_update)
                conn.commit()
                print(f'Updated {entry["ID"]}')
            except sqlite3.Error as e:
                #print(f'Failed to update {entry["ID"]}: {entry["TASK"]} with statement {sql_update} \n Error: {e}')
                print(sql_update)
                print(e)
            finally:
                if cur:
                    cur.close()


def parse_filter(filter):
    statement = 'WHERE '
    sections = filter.split(';')
    filters = []

    for section in sections:
        filter_field = re.search('.*(?=\:)', section).group(0).strip()
        filter_query = re.search('(?<=\:).*', section).group(0).strip()
 
        if filter_field.lower() in LOG_TEMP.keys():
            if 'range' in filter_query:
                # add timefilter query
                range = parse_timefilter(filter_query)
                filters.append(f"{filter_field.upper()}  BETWEEN {range['start']} AND {range['end']}")
            elif '~' in filter_query:
                filters.append(f'{filter_field.upper()} {parse_regexfilter(filter_query)}')
        else:
            print(f'Skipped {filter_field}, not a valid field to apply filter.')
    statement += ' AND '.join(filters)
    return statement


def parse_timefilter(query, **kwargs):
    # define interval set and timeset
    ins = {
        's': 1,
        'm': 60,
        'h': 3600,
        'd': 86400
    }
    ts = {}
    # allow an overwrite of now for unit tests
    if 'now' in kwargs and isinstance(kwargs['now'], datetime):
        ts['now'] = kwargs['now']
    else:
        ts['now'] = datetime.now()

    # populate timeset 
    ts['start_of_today'] = datetime(
        year = ts['now'].year,
        month = ts['now'].month,
        day = ts['now'].day,
        hour = 0,
        minute = 0,
        second = 0,
    )
    ts['end_of_today'] = ts['start_of_today'] + timedelta(hours=23, minutes=59, seconds=59)

    ts['start_of_this_month'] = datetime(
        year = ts['now'].year,
        month = ts['now'].month,
        day = 1,
        hour = 0,
        minute = 0,
        second = 0,
    )

    ts['end_of_this_month'] = ts['start_of_this_month'] + timedelta(days=(monthrange(ts['now'].year, ts['now'].month)[1] - 1), hours=23, minutes=59, seconds=59)
    ts['start_of_this_week'] = ts['start_of_today'] - timedelta(days=ts['end_of_today'].weekday())
    ts['end_of_this_week'] = ts['start_of_this_week'] + timedelta(days=6, minutes=59, seconds = 59)

    # default range to from 1970-01-01 to now
    range = {'stat': 0, 'end': int(ts['now'].timestamp())}

    # regex match query to get start and end
    query_content = re.sub('\s*range\s*=\s*', '', query).strip()

    query_range = {}
    (start, end) = query_content.split(' to ')
    query_range = {'start': start.strip(), 'end': end.strip()}

    for k in query_range.keys():
        # if the key is already in timeset, apply it
        if query_range[k] in ts:
            query_range[k] = int(ts[query_range[k]].timestamp())
        elif any([sign in query_range[k] for sign in ['+', '-']]):
            operands = [
                re.search('.*(?=[\-,\+])', query_range[k]).group(0).strip(),
                re.search('(?<=[\-,\+]).*', query_range[k]).group(0).strip()
            ]
            operator = re.search('[\-,\+]', query_range[k]).group(0).strip()

            if operands[0] in ts:
                operands[0] = int(ts[operands[0]].timestamp())
            elif operands[0].isnumeric():
                operands[0] = int(ts[operands[0]])
            else:
                print(f'invalid time operand: {operands[0]}')

            for i in ins.keys():
                if i in operands[1]:
                    try:
                        operands[1] = eval(operands[1].replace(i, f'* {ins[i]}'))
                        break
                    except Exception as e:
                        print(e)
                        exit(1)
            try:
                query_range[k] = eval(' '.join([str(operands[0]), operator, str(operands[1])]))
            except Exception as e:
                print(e)
                exit(1)
        else:
            print(f'Invalid timefilter range: {query_content}')
            exit(1)

    if any([not isinstance(query_range[k], int) for k in ['start', 'end']]):
        print(f'Invalid timefilter range: {query_content}')
        exit(1)

    return(query_range)

def parse_regexfilter(query, **kwargs):
    query_content = re.sub('\s*~\s*', '', query).strip()
    return f"LIKE '{query_content}'"


if __name__ == '__main__':
    simple_note()

#
# Unit testing
#
def test_parse_timefilter():
   assert parse_timefilter('range= now - 5h to end_of_today', now=datetime(2022, 4, 7, 12, 30, 00)) == {'start': int(datetime(2022, 4, 7, 7, 30, 0).timestamp()), 'end': int(datetime(2022, 4, 7, 23, 59, 59).timestamp())}
   assert parse_timefilter('range=start_of_today - 1d to end_of_this_month + 3m', now=datetime(2022, 4, 7, 12, 30, 00)) == {'start': int(datetime(2022, 4, 6, 0, 0, 0).timestamp()), 'end': int(datetime(2022, 5, 1, 0, 2, 59).timestamp())}
   assert parse_timefilter('range =start_of_today - 1d to end_of_this_month + 3m', now=datetime(2022, 4, 7, 12, 30, 00)) == {'start': int(datetime(2022, 4, 6, 0, 0, 0).timestamp()), 'end': int(datetime(2022, 5, 1, 0, 2, 59).timestamp())}
   assert parse_timefilter('range= start_of_this_week - 300m to start_of_today + 7s', now=datetime(2022, 4, 7, 12, 30, 00)) == {'start': int(datetime(2022, 4, 3, 19, 0, 0).timestamp()), 'end': int(datetime(2022, 4, 7, 0, 0, 7).timestamp())}

