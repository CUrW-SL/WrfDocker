import getopt
import glob
import json
import ntpath
import os
import shlex
import shutil
import subprocess
import sys
import traceback
from datetime import datetime, time
from zipfile import ZipFile, ZIP_DEFLATED
import constants


def get_incremented_dir_path(path):
    """
    returns the incremented dir path
    ex: /a/b/c/0 if not exists returns /a/b/c/0 else /a/b/c/1
    :param path:
    :return:
    """
    while os.path.exists(path):
        try:
            base = str(int(os.path.basename(path)) + 1)
            path = os.path.join(os.path.dirname(path), base)
        except ValueError:
            path = os.path.join(path, '0')
    return path


def backup_dir(path):
    bck_str = '__backup'
    if os.path.exists(path):
        bck_files = [l for l in os.listdir(path) if bck_str not in l]
        if len(bck_files) > 0:
            bck_dir = get_incremented_dir_path(os.path.join(path, bck_str))
            os.makedirs(bck_dir)
            for file in bck_files:
                shutil.move(os.path.join(path, file), bck_dir)
            return bck_dir
    return None


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def copy_files_with_prefix(src_dir, prefix, dest_dir):
    create_dir_if_not_exists(dest_dir)
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        shutil.copy(filename, os.path.join(dest_dir, ntpath.basename(filename)))


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


def get_em_real_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, constants.DEFAULT_EM_REAL_PATH)


def run_em_real(wrf_config):
    print('Running em_real...')

    wrf_home = wrf_config['wrf_home']
    em_real_dir = get_em_real_dir(wrf_home)
    procs = wrf_config['procs']
    run_id = wrf_config['run_id']
    output_dir = create_dir_if_not_exists(os.path.join(wrf_config['nfs_dir'], 'results', run_id, 'wrf'))
    archive_dir = create_dir_if_not_exists(os.path.join(wrf_config['archive_dir'], 'results', run_id, 'wrf'))

    print('run_em_real|output_dir: ', output_dir)
    print('run_em_real|archive_dir: ', archive_dir)

    print('Backup the output dir')
    backup_dir(output_dir)

    logs_dir = create_dir_if_not_exists(os.path.join(output_dir, 'logs'))

    print('Copying metgrid.zip')
    metgrid_dir = os.path.join(wrf_config['nfs_dir'], 'metgrid')

    copy_files_with_prefix(metgrid_dir, wrf_config['run_id'] + '_metgrid.zip', em_real_dir)
    metgrid_zip = os.path.join(em_real_dir, wrf_config['run_id'] + '_metgrid.zip')

    print('Extracting metgrid.zip')
    ZipFile(metgrid_zip, 'r', compression=ZIP_DEFLATED).extractall(path=em_real_dir)

    # logs destination: nfs/logs/xxxx/rsl*
    try:
        try:
            print('Starting real.exe')
            print('em_real_dir : ', em_real_dir)
            run_subprocess('mpirun -np %d ./real.exe' % procs, cwd=em_real_dir)
        finally:
            print('Moving Real log files...')
            create_zip_with_prefix(em_real_dir, 'rsl*', os.path.join(em_real_dir, 'real_rsl.zip'), clean_up=True)
            move_files_with_prefix(em_real_dir, 'real_rsl.zip', logs_dir)
        try:
            print('Starting wrf.exe')
            run_subprocess('mpirun -np %d ./wrf.exe' % procs, cwd=em_real_dir)
        finally:
            print('Moving WRF log files...')
            create_zip_with_prefix(em_real_dir, 'rsl*', os.path.join(em_real_dir, 'wrf_rsl.zip'), clean_up=True)
            move_files_with_prefix(em_real_dir, 'wrf_rsl.zip', logs_dir)
    finally:
        print('Moving namelist input file')
        move_files_with_prefix(em_real_dir, 'namelist.input', output_dir)

    print('WRF em_real: DONE! Moving data to the output dir')

    print('Extracting rf from domain3')
    d03_nc = glob.glob(os.path.join(em_real_dir, 'wrfout_d03_*'))[0]
    ncks_query = 'ncks -v %s %s %s' % ('RAINC,RAINNC,XLAT,XLONG,Times', d03_nc, d03_nc + '_rf.nc')
    run_subprocess(ncks_query)

    print('Extracting rf from domain1')
    d01_nc = glob.glob(os.path.join(em_real_dir, 'wrfout_d01_*'))[0]
    ncks_query = 'ncks -v %s %s %s' % ('RAINC,RAINNC,XLAT,XLONG,Times', d01_nc, d01_nc + '_rf.nc')
    run_subprocess(ncks_query)

    print('Moving data to the output dir')
    move_files_with_prefix(em_real_dir, 'wrfout_d03*_rf.nc', output_dir)
    move_files_with_prefix(em_real_dir, 'wrfout_d01*_rf.nc', output_dir)
    print('Moving data to the archive dir')
    move_files_with_prefix(em_real_dir, 'wrfout_*', archive_dir)

    print('Cleaning up files')
    delete_files_with_prefix(em_real_dir, 'met_em*')
    delete_files_with_prefix(em_real_dir, 'rsl*')
    os.remove(metgrid_zip)


def run_subprocess(cmd, cwd=None, print_stdout=False):
    print('Running subprocess %s cwd %s' % (cmd, cwd))
    print('Running subprocess %s cwd %s' % (cmd, cwd))
    start_t = time.time()
    output = ''
    try:
        output = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, cwd=cwd)
    except subprocess.CalledProcessError as e:
        print('Exception in subprocess %s! Error code %d' % (cmd, e.returncode))
        print(e.output)
        raise e
    finally:
        elapsed_t = time.time() - start_t
        print('Subprocess %s finished in %f s' % (cmd, elapsed_t))
        if print_stdout:
            print('stdout and stderr of %s\n%s' % (cmd, output))
    return output


def get_wps_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, constants.DEFAULT_WPS_PATH)


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
        config = json.load(json_file)
        gfs_data_path = os.path.join(path, 'wrf{}/d{}/{}/gfs/{}'.format(workflow, run_day, data_hour, run_date))
        wps_path = os.path.join(path, 'wrf{}/d{}/{}/wps/{}'.format(workflow, run_day, data_hour, run_date))
        if os.path.exists(path):
            gfs_date = '{}_{}:00'.format(run_date, data_hour)
            config['gfs_date'] = gfs_date
            namelist_updated_path = os.path.join(path, 'wrf{}/d{}/{}/{}/{}'.format(workflow,
                                                 run_day, data_hour, model, run_date), 'namelist.input')
            config['namelist_updated'] = namelist_updated_path
            wps_dir = get_wps_dir(config['wrf_home'])
            print('wps_dir : ', wps_dir)
            shutil.rmtree(config['gfs_dir'])
            delete_files_with_prefix(wps_dir, 'FILE:*')
            delete_files_with_prefix(wps_dir, 'PFILE:*')
            delete_files_with_prefix(wps_dir, 'geo_em.*')
            run_em_real(config)
except Exception as e:
    traceback.print_exc()

