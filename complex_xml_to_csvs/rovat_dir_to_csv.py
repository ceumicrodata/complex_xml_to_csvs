__doc__ = '''\
Concatenate csv files under a ROVAT_DIR directory to a ROVAT_DIR.csv
and remove the directory.

Usage:
    {} ROVAT_DIR

Where ROVAT_DIR is the directory
'''.format(__file__)

USAGE = __doc__


import os
import sys
import subprocess
import shutil


def die_with(message):
    sys.stderr.write(message)
    sys.exit(1)


def main():
    try:
        rovat_dir, = sys.argv[1:]
        rovat_dir = os.path.abspath(rovat_dir)
    except:
        die_with(USAGE)

    if not os.path.isdir(rovat_dir):
        die_with('{} is not a directory'.format(rovat_dir))

    files = sorted(os.listdir(rovat_dir))
    if not files:
        os.rmdir(rovat_dir)
        print('WARNING: {} was empty'.format(rovat_dir))
        return

    csv_name = '{}.csv'.format(rovat_dir)
    with open(csv_name, 'w') as csv_file:
        subprocess.check_call(
            ['csv_cat'] + files,
            stdout=csv_file,
            close_fds=True,
            cwd=rovat_dir
        )
        shutil.rmtree(rovat_dir)


if __name__ == '__main__':
    main()
