# -*- coding: utf-8 -*-

DESCRIPTION = """convert edgelist from parquet format to pajek"""

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

from h1theswan_utils.network_data import PajekFactory

from config import Config
config = Config()
spark = config.spark

def create_pajek_from_spark_dataframe(sdf, column_names=['UID', 'cited_UID']):
    pjk = PajekFactory()
    rownum = 0
    for row in sdf.rdd.toLocalIterator():
        pjk.add_edge(row[column_names[0]], row[column_names[1]])
        rownum += 1
        if (rownum in [1,5,10,50,100,1000,10000,100000,1e6,10e6] or (rownum % 50e6 == 0)):
            logger.debug('{} edges added'.format(rownum))
    return pjk

def write_pajek_to_file(pjk, outfname):
    with open(outfname, 'w') as outf:
        pjk.write(outf, vertices_label='Vertices', edges_label='Arcs')

def main(args):
    logger.debug('loading input file {}'.format(args.input))
    sdf = spark.read.parquet(args.input)

    logger.debug('converting to pajek...')
    pjk = create_pajek_from_spark_dataframe(sdf)

    logger.debug('writing to file {}'.format(args.output))
    write_pajek_to_file(pjk, args.output)


if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("input", help="path to input (parquet)")
    parser.add_argument("output", help="path to output (pajek .net)")
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
