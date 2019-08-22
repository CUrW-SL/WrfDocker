import glob
import json
import logging
import math
import multiprocessing
import ntpath
import os
import re
import shlex
import shutil
import subprocess
import traceback
from datetime import datetime, timedelta
import time
import getopt
import sys
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from zipfile import ZipFile

import pkg_resources
from joblib import Parallel, delayed

LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
logging.basicConfig(filename='/mnt/disks/data/logs/run_wps.log',
                    level=logging.DEBUG,
                    format=LOG_FORMAT)
log = logging.getLogger()


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
def run_subprocess(cmd, cwd=None, print_stdout=False):
    print('Running subprocess %s cwd %s' % (cmd, cwd))
    log.info('Running subprocess %s cwd %s' % (cmd, cwd))
    start_t = time.time()
    output = ''
    try:
        output = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, cwd=cwd)
    except subprocess.CalledProcessError as e:
        print('Exception in subprocess %s! Error code %d' % (cmd, e.returncode))
        log.error('Exception in subprocess %s! Error code %d' % (cmd, e.returncode))
        print(e.output)
        log.error(e.output)
        raise e
    finally:
        elapsed_t = time.time() - start_t
        print('Subprocess %s finished in %f s' % (cmd, elapsed_t))
        log.info('Subprocess %s finished in %f s' % (cmd, elapsed_t))
        if print_stdout:
            print('stdout and stderr of %s\n%s' % (cmd, output))
            log.info('stdout and stderr of %s\n%s' % (cmd, output))
    return output


def get_wps_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, constants.DEFAULT_WPS_PATH)


def replace_file_with_values(source, destination, val_dict):
    log.debug('replace file source ' + source)
    log.debug('replace file destination ' + destination)
    log.debug('replace file content dict ' + str(val_dict))
    # pattern = re.compile(r'\b(' + '|'.join(val_dict.keys()) + r')\b')
    pattern = re.compile('|'.join(list(val_dict.keys())))

    with open(destination, 'w') as dest:
        out = ''
        with open(source, 'r') as src:
            line = pattern.sub(lambda x: val_dict[x.group()], src.read())
            dest.write(line)
            out += line

    log.debug('replace file final content \n' + out)


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
    log.info('Replacing namelist.wps...')
    if os.path.exists(wrf_config['namelist_wps']):
        f = wrf_config['namelist_wps']
    else:
        f = get_resource_path(os.path.join('execution', constants.DEFAULT_NAMELIST_WPS_TEMPLATE))

    dest = os.path.join(get_wps_dir(wrf_config['wrf_home']), 'namelist.wps')
    print('replace_namelist_wps|dest: ', dest)
    start_date = datetime.strptime(wrf_config['start_date'], '%Y-%m-%d_%H:%M')
    replace_file_with_values_with_dates(wrf_config, f, dest, 'namelist_wps_dict', start_date, end_date)


def create_dir_if_not_exists(path):
    """
    create directory(if needed recursively) or paths
    :param path: string : directory path
    :return: string
    """
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def move_files_with_prefix(src_dir, prefix, dest_dir):
    create_dir_if_not_exists(dest_dir)
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        shutil.move(filename, os.path.join(dest_dir, ntpath.basename(filename)))


def check_geogrid_output(wps_dir):
    for i in range(1, 4):
        if not os.path.exists(os.path.join(wps_dir, 'geo_em.d%02d.nc' % i)):
            return False
    return True


def move_files_with_prefix(src_dir, prefix, dest_dir):
    create_dir_if_not_exists(dest_dir)
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        shutil.move(filename, os.path.join(dest_dir, ntpath.basename(filename)))


def create_zip_with_prefix(src_dir, regex, dest_zip, comp=ZIP_DEFLATED, clean_up=False):
    with ZipFile(dest_zip, 'w', compression=comp) as zip_file:
        for filename in glob.glob(os.path.join(src_dir, regex)):
            zip_file.write(filename, arcname=os.path.basename(filename))
            if clean_up:
                os.remove(filename)
    return dest_zip


def delete_files_with_prefix(src_dir, prefix):
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        os.remove(filename)


def get_appropriate_gfs_inventory(wrf_config):
    st = datetime_floor(datetime.strptime(wrf_config['start_date'], '%Y-%m-%d_%H:%M'), 3600 * wrf_config['gfs_step'])
    # if the time difference between now and start time is lt gfs_lag, then the time will be adjusted
    if (datetime.utcnow() - st).total_seconds() <= wrf_config['gfs_lag'] * 3600:
        floor_val = datetime_floor(st - timedelta(hours=wrf_config['gfs_lag']), 6 * 3600)
    else:
        floor_val = datetime_floor(st, 6 * 3600)
    gfs_date = floor_val.strftime('%Y%m%d')
    gfs_cycle = str(floor_val.hour).zfill(2)
    start_inv = math.floor((st - floor_val).total_seconds() / 3600 / wrf_config['gfs_step']) * wrf_config['gfs_step']
    return gfs_date, gfs_cycle, start_inv


def get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, fcst_id, res, gfs_dir):
    url0 = url.replace('YYYY', date_str[0:4]).replace('MM', date_str[4:6]).replace('DD', date_str[6:8]).replace('CC',
                                                                                                                cycle)
    inv0 = inv.replace('CC', cycle).replace('FFF', fcst_id).replace('RRRR', res).replace('YYYY', date_str[0:4]).replace(
        'MM', date_str[4:6]).replace('DD', date_str[6:8])

    dest = os.path.join(gfs_dir, date_str + '.' + inv0)
    return url0 + inv0, dest


def run_wps(wrf_config):
    log.info('Running WPS: START')
    wps_dir = wrf_config['wps_dir']
    output_dir = wps_dir
    print('run_wps|output_dir : ', output_dir)

    log.info('Cleaning up files')
    logs_dir = create_dir_if_not_exists(os.path.join(output_dir, 'logs'))

    delete_files_with_prefix(wps_dir, 'FILE:*')
    delete_files_with_prefix(wps_dir, 'PFILE:*')
    delete_files_with_prefix(wps_dir, 'met_em*')

    # Linking VTable
    if not os.path.exists(os.path.join(wps_dir, 'Vtable')):
        print('Creating Vtable symlink')
        log.info('Creating Vtable symlink')
        os.symlink(os.path.join(wps_dir, 'ungrib/Variable_Tables/Vtable.NAM'), os.path.join(wps_dir, 'Vtable'))
        print('symlinks has created.')

    # Running link_grib.csh
    gfs_date, gfs_cycle, start = get_appropriate_gfs_inventory(wrf_config)
    dest = get_gfs_data_url_dest_tuple(wrf_config['gfs_url'], wrf_config['gfs_inv'], gfs_date, gfs_cycle,
                                       '', wrf_config['gfs_res'], '')[1].replace('.grb2', '')
    print('----------------------gfs_dir : ', wrf_config['gfs_dir'])
    print('----------------------wps_dir : ', wps_dir)
    print('----------------------dest : ', dest)
    run_subprocess(
        'csh link_grib.csh %s/%s' % (wrf_config['gfs_dir'], dest), cwd=wps_dir)
    try:
        # Starting ungrib.exe
        try:
            run_subprocess('./ungrib.exe', cwd=wps_dir)
        finally:
            move_files_with_prefix(wps_dir, 'ungrib.log', logs_dir)
        # Starting geogrid.exe'
        if not check_geogrid_output(wps_dir):
            logging.info('Geogrid output not available')
            try:
                run_subprocess('./geogrid.exe', cwd=wps_dir)
            finally:
                move_files_with_prefix(wps_dir, 'geogrid.log', logs_dir)
        # Starting metgrid.exe'
        try:
            run_subprocess('./metgrid.exe', cwd=wps_dir)
        finally:
            move_files_with_prefix(wps_dir, 'metgrid.log', logs_dir)
    finally:
        log.info('Moving namelist wps file')
        move_files_with_prefix(wps_dir, 'namelist.wps', output_dir)

    log.info('Running WPS: DONE')

    log.info('Zipping metgrid data')
    metgrid_zip = os.path.join(wps_dir,'metgrid.zip')
    create_zip_with_prefix(wps_dir, 'met_em.d*', metgrid_zip)

    log.info('Moving metgrid data')
    dest_dir = os.path.join(wrf_config['nfs_dir'], 'metgrid')
    move_files_with_prefix(wps_dir, metgrid_zip, dest_dir)


try:
    print('WPS process triggered...')
    workflow = '1'
    run_day = '0'
    data_hour = '00'
    model = ''
    run_date = ''
    path = '/mnt/disks/data/wrf_run'
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:m:w:d:p:", [
            "hour=", "model=", "workflow=", "run_date=", "path="
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
    print("GFS data hour : ", data_hour)
    print("GFS run_date : ", run_date)
    with open('config.json') as json_file:
        wps_config = json.load(json_file)
        gfs_data_path = os.path.join(path, 'wrf{}/d{}/{}/gfs/{}'.format(workflow, run_day, data_hour, run_date))
        wps_path = os.path.join(path, 'wrf{}/d{}/{}/wps/{}'.format(workflow, run_day, data_hour, run_date))
        create_dir_if_not_exists(wps_path)
        if os.path.exists(path):
            wps_config['gfs_dir'] = gfs_data_path
            wps_config['wps_dir'] = wps_path
            gfs_date = '{}_{}:00'.format(run_date, data_hour)
            wps_config['gfs_date'] = gfs_date
            run_wps(wps_config)
except Exception as e:
    traceback.print_exc()

