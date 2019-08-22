import json
import logging
import math
import multiprocessing
import os
import shutil
import traceback
from datetime import datetime, timedelta
import time
import getopt
import sys
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from joblib import Parallel, delayed

LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
logging.basicConfig(filename='/mnt/disks/data/logs/gfs_data.log',
                    level=logging.DEBUG,
                    format=LOG_FORMAT)
log = logging.getLogger()


def create_dir_if_not_exists(path):
    """
    create directory(if needed recursively) or paths
    :param path: string : directory path
    :return: string
    """
    if not os.path.exists(path):
        os.makedirs(path)


def file_exists_nonempty(filename):
    return os.path.exists(filename) and os.path.isfile(filename) and os.stat(filename).st_size != 0


def download_file(url, dest, retries=0, delay=60, overwrite=False, secondary_dest_dir=None):
    try_count = 1
    last_e = None

    def _download_file(_url, _dest):
        _f = urlopen(_url)
        with open(_dest, "wb") as _local_file:
            _local_file.write(_f.read())
            print('Downloaded {}'.format(_url))

    while try_count <= retries + 1:
        try:
            print("Downloading %s to %s" % (url, dest))
            log.info("Downloading %s to %s" % (url, dest))
            if secondary_dest_dir is None:
                if not overwrite and file_exists_nonempty(dest):
                    print('File already exists. Skipping download!')
                    log.info('File already exists. Skipping download!')
                else:
                    _download_file(url, dest)
                return
            else:
                secondary_file = os.path.join(secondary_dest_dir, os.path.basename(dest))
                if file_exists_nonempty(secondary_file):
                    print("File available in secondary dir. Copying to the destination dir from secondary dir")
                    log.info("File available in secondary dir. Copying to the destination dir from secondary dir")
                    shutil.copyfile(secondary_file, dest)
                else:
                    print("File not available in secondary dir. Downloading...")
                    log.info("File not available in secondary dir. Downloading...")
                    _download_file(url, dest)
                    print("Copying to the secondary dir")
                    log.info("Copying to the secondary dir")
                    shutil.copyfile(dest, secondary_file)
                return

        except (HTTPError, URLError) as e:
            print(
                'Error in downloading %s Attempt %d : %s . Retrying in %d seconds' % (url, try_count, e.message, delay))
            log.error(
                'Error in downloading %s Attempt %d : %s . Retrying in %d seconds' % (url, try_count, e.message, delay))
            try_count += 1
            last_e = e
            time.sleep(delay)
        except FileExistsError:
            print('File was already downloaded by another process! Returning')
            log.info('File was already downloaded by another process! Returning')
            return
    raise last_e


def download_parallel(url_dest_list, procs=multiprocessing.cpu_count(), retries=0, delay=60, overwrite=False,
                      secondary_dest_dir=None):
    Parallel(n_jobs=procs)(
        delayed(download_file)(i[0], i[1], retries, delay, overwrite, secondary_dest_dir) for i in url_dest_list)


def get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, fcst_id, res, gfs_dir):
    url0 = url.replace('YYYY', date_str[0:4]).replace('MM', date_str[4:6]).replace('DD', date_str[6:8]).replace('CC',
                                                                                                                cycle)
    inv0 = inv.replace('CC', cycle).replace('FFF', fcst_id).replace('RRRR', res).replace('YYYY', date_str[0:4]).replace(
        'MM', date_str[4:6]).replace('DD', date_str[6:8])

    dest = os.path.join(gfs_dir, date_str + '.' + inv0)
    return url0 + inv0, dest


def get_gfs_inventory_url_dest_list(date, period, url, inv, step, cycle, res, gfs_dir, start=0):
    date_str = date.strftime('%Y%m%d') if type(date) is datetime else date
    return [get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, str(i).zfill(3), res, gfs_dir) for i in
            range(start, start + int(period * 24) + 1, step)]


def datetime_to_epoch(timestamp=None):
    timestamp = datetime.now() if timestamp is None else timestamp
    return (timestamp - datetime(1970, 1, 1)).total_seconds()


def epoch_to_datetime(epoch_time):
    return datetime(1970, 1, 1) + timedelta(seconds=epoch_time)


def datetime_floor(timestamp, floor_sec):
    return epoch_to_datetime(math.floor(datetime_to_epoch(timestamp) / floor_sec) * floor_sec)


def get_appropriate_gfs_inventory(wrf_config):
    st = datetime_floor(datetime.strptime(wrf_config['gfs_date'], '%Y-%m-%d_%H:%M'), 3600 * wrf_config['gfs_step'])
    # if the time difference between now and start time is lt gfs_lag, then the time will be adjusted
    if (datetime.utcnow() - st).total_seconds() <= wrf_config['gfs_lag'] * 3600:
        floor_val = datetime_floor(st - timedelta(hours=wrf_config['gfs_lag']), 6 * 3600)
    else:
        floor_val = datetime_floor(st, 6 * 3600)
    gfs_date = floor_val.strftime('%Y%m%d')
    gfs_cycle = str(floor_val.hour).zfill(2)
    start_inv = math.floor((st - floor_val).total_seconds() / 3600 / wrf_config['gfs_step']) * wrf_config['gfs_step']

    return gfs_date, gfs_cycle, start_inv


def download_gfs_data(gfs_config):
    """
    :param start_date: '2017-08-27_00:00'
    :return:
    """
    log.info('Downloading GFS data: START')
    try:
        gfs_date, gfs_cycle, start_inv = get_appropriate_gfs_inventory(gfs_config)
        inventories = get_gfs_inventory_url_dest_list(gfs_date, gfs_config['period'],
                                                      gfs_config['gfs_url'],
                                                      gfs_config['gfs_inv'], gfs_config['gfs_step'],
                                                      gfs_cycle, gfs_config['gfs_res'],
                                                      gfs_config['gfs_download_path'], start=start_inv)
        gfs_threads = gfs_config['gfs_threads']
        log.info('Following data will be downloaded in %d parallel threads\n%s' % (gfs_threads, '\n'.join(
            ' '.join(map(str, i)) for i in inventories)))

        start_time = time.time()
        download_parallel(inventories, procs=gfs_threads, retries=gfs_config['gfs_retries'],
                          delay=gfs_config['gfs_delay'], secondary_dest_dir=None)

        elapsed_time = time.time() - start_time
        log.info('Downloading GFS data: END Elapsed time: %f' % elapsed_time)
        log.info('Downloading GFS data: END')
        return gfs_date, start_inv
    except Exception as e:
        log.error('Downloading GFS data error: {}'.format(str(e)))


try:
    print('GFS data downloading process triggered...')
    workflow = '1'
    run_day = '0'
    data_hour = '00'
    model = ''
    run_date = ''
    path = '/mnt/disks/data/wrf'
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
        gfs_config = json.load(json_file)
        gfs_download_path = os.path.join(path, 'wrf{}/d{}/{}/gfs/{}'.format(workflow, run_day, data_hour, run_date))
        create_dir_if_not_exists(gfs_download_path)
        gfs_config['gfs_download_path'] = gfs_download_path
        gfs_date = '2019-08-03_00:00'
        gfs_date = '{}_{}:00'.format(run_date, data_hour)
        gfs_config['gfs_date'] = gfs_date
        download_gfs_data(gfs_config)
except Exception as e:
    traceback.print_exc()

