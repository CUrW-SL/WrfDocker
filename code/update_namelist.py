import math
import re
from datetime import datetime, timedelta
import getopt
import json
import os
import sys
import traceback
import pkg_resources


class UnableFindResource(Exception):
    def __init__(self, res):
        Exception.__init__(self, 'Unable to find %s' % res)


def get_resource_path(resource):
    res = pkg_resources.resource_filename(__name__, resource)
    if os.path.exists(res):
        return res
    else:
        raise UnableFindResource(resource)


def datetime_to_epoch(timestamp=None):
    timestamp = datetime.now() if timestamp is None else timestamp
    return (timestamp - datetime(1970, 1, 1)).total_seconds()


def epoch_to_datetime(epoch_time):
    return datetime(1970, 1, 1) + timedelta(seconds=epoch_time)


def datetime_floor(timestamp, floor_sec):
    return epoch_to_datetime(math.floor(datetime_to_epoch(timestamp) / floor_sec) * floor_sec)


def replace_file_with_values(source, destination, val_dict):
    print('replace file source ' + source)
    print('replace file destination ' + destination)
    print('replace file content dict ' + str(val_dict))
    # pattern = re.compile(r'\b(' + '|'.join(val_dict.keys()) + r')\b')
    pattern = re.compile('|'.join(list(val_dict.keys())))

    with open(destination, 'w') as dest:
        out = ''
        with open(source, 'r') as src:
            line = pattern.sub(lambda x: val_dict[x.group()], src.read())
            dest.write(line)
            out += line

    print('replace file final content \n' + out)


def replace_file_with_values_with_dates(wrf_config, src, dest, aux_dict, start_date=None, end_date=None):
    if start_date is None:
        start_date = datetime_floor(datetime.strptime(wrf_config['start_date'], '%Y-%m-%d_%H:%M'),
                                    wrf_config['gfs_step'] * 3600)

    if end_date is None:
        end_date = start_date + timedelta(days=wrf_config['period'])

    period = wrf_config['period']

    d = {
        'YYYY1': start_date.strftime('%Y'),
        'MM1': start_date.strftime('%m'),
        'DD1': start_date.strftime('%d'),
        'hh1': start_date.strftime('%H'),
        'mm1': start_date.strftime('%M'),
        'YYYY2': end_date.strftime('%Y'),
        'MM2': end_date.strftime('%m'),
        'DD2': end_date.strftime('%d'),
        'hh2': end_date.strftime('%H'),
        'mm2': end_date.strftime('%M'),
        'GEOG': wrf_config['geog_dir'],
        'RD0': str(int(period)),
        'RH0': str(int(period * 24 % 24)),
        'RM0': str(int(period * 60 * 24 % 60)),
        'hi1': '180',
        'hi2': '60',
        'hi3': '60',
    }

    if aux_dict and aux_dict in wrf_config:
        d.update(wrf_config[aux_dict])

    replace_file_with_values(src, dest, d)


def replace_namelist_wps(wrf_config, start_date=None, end_date=None):
    print('Replacing namelist.wps...')
    if os.path.exists(wrf_config['namelist_wps']):
        f = wrf_config['namelist_wps']
    else:
        f = get_resource_path(os.path.join('execution', constants.DEFAULT_NAMELIST_WPS_TEMPLATE))

    dest = os.path.join(get_wps_dir(wrf_config['wrf_home']), 'namelist.wps')
    print('replace_namelist_wps|dest: ', dest)
    start_date = datetime.strptime(wrf_config['start_date'], '%Y-%m-%d_%H:%M')
    replace_file_with_values_with_dates(wrf_config, f, dest, 'namelist_wps_dict', start_date, end_date)


try:
    print('WPS process triggered...')
    workflow = '1'
    run_day = '0'
    data_hour = '00'
    model = ''
    run_date = ''
    path = '/mnt/disks/data/wrf_run'
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:m:w:d:p:n:", [
            "hour=", "model=", "workflow=", "run_date=", "path=", "namelist="
        ])
    except getopt.GetoptError:
        print('Input error.')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--hour"):
            data_hour = arg  # '00'|'06'|'12'|'18'
        elif opt in ("-m", "--model"):
            model = arg  # 'A'|'C'|'E'|'SE'
        elif opt in ("-w", "--workflow"):
            workflow = arg  # '0'|'1'
        elif opt in ("-p", "--path"):
            path = arg  #
        elif opt in ("-d", "--run_date"):
            run_date = arg  # '2019-08-21'
        elif opt in ("-n", "--namelist"):
            namelist = arg  # 'wps'|'wrf'
    print("GFS data hour : ", data_hour)
    print("GFS run_date : ", run_date)
    print("namelist : ", namelist)
    namelist_path = ''
    with open('config.json') as json_file:
        config = json.load(json_file)
        gfs_data_path = os.path.join(path, 'wrf{}/d{}/{}/gfs/{}'.format(workflow, run_day, data_hour, run_date))
        wps_path = os.path.join(path, 'wrf{}/d{}/{}/wps/{}'.format(workflow, run_day, data_hour, run_date))
        gfs_date = '{}_{}:00'.format(run_date, data_hour)
        if namelist == 'wps':
            namelist_path = os.path.join(path, 'template', 'namelist.wps')
            config['namelist'] = namelist_path
        elif namelist == 'wrf':
            namelist_path = os.path.join(path, 'template', 'namelist.input')
            config['namelist'] = namelist_path
except Exception as e:
    traceback.print_exc()

