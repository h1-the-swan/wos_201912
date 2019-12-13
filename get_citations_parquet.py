# -*- coding: utf-8 -*-

DESCRIPTION = """Combine TSV files (exported from mysql), dedup, and save to parquet"""

import sys, os, time
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)

import logging
logging.basicConfig(format='%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s',
        datefmt="%H:%M:%S",
        level=logging.INFO)
# logger = logging.getLogger(__name__)
logger = logging.getLogger('__main__').getChild(__name__)

from config import Config
config = Config()
spark = config.spark

def load_input_files(fnames):
    """load input files into spark

    :fnames: TSV filenames (with header)
    :returns: spark dataframe

    """
    sdf_combined = None
    for fname in fnames:
        _sdf = spark.read.csv(fname, sep='\t', header=True)
        if sdf_combined is None:
            sdf_combined = _sdf
        else:
            sdf_combined = sdf_combined.union(_sdf)
    return sdf_combined

def main(args):
    logger.debug('reading {} input files'.format(len(args.input)))
    sdf = load_input_files(args.input)
    logger.debug('removing rows with `cited_UID=="NULL"`')
    sdf = sdf[sdf['cited_UID']!='NULL']
    logger.debug('dropping duplicates')
    sdf = sdf.drop_duplicates()
    logger.debug('saving to {}'.format(args.output))
    sdf.write.parquet(args.output)

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("-i", "--input", nargs='+', required=True, help="input files (TSV with header)")
    parser.add_argument("-o", "--output", required=True, help="name of output (parquet)")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    else:
        logger.setLevel(logging.INFO)
    main(args)
    total_end = timer()
    logger.info('all finished. total time: {}'.format(format_timespan(total_end-total_start)))
