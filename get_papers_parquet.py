# -*- coding: utf-8 -*-

DESCRIPTION = """Combine wos and wos2 papers with cluster and flow information and save to parquet (using Spark)"""

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

def load_sdf_clusters(fname):
    """Load clusters data as a spark dataframe

    :fname: path to TSV file
    :returns: spark dataframe

    """
    schema = 'node_id bigint, node_name string, cl string, cl_top bigint, flow float'
    sdf = spark.read.csv(fname, sep='\t', header=True, schema=schema)
    return sdf

def load_papers(fnames):
    """Load papers as a spark dataframe

    :fnames: TSV filenames (with header)
    :returns: spark dataframe

    """
    sdf_combined = None
    columns_to_keep = ['UID', 'title', 'pub_date', 'doi', 'title_source', 'subject_extended', 'subject_traditional']
    for fname in fnames:
        _sdf = spark.read.csv(fname, sep='\t', header=True)
        _sdf = _sdf.select(columns_to_keep)
        # change pub_date column datatype to DateType
        _sdf = _sdf.withColumn('pub_date', _sdf['pub_date'].cast("date"))
        if sdf_combined is None:
            sdf_combined = _sdf
        else:
            sdf_combined = sdf_combined.union(_sdf)
    return sdf_combined

def main(args):
    sdf_papers = load_papers(args.papers)
    sdf_clusters = load_sdf_clusters(args.clusters)

    sdf_clusters = sdf_clusters.withColumnRenamed('node_name', 'UID')
    sdf_papers = sdf_papers.join(sdf_clusters, on='UID', how='left')

    logger.debug('dropping duplicates')
    sdf_papers = sdf_papers.drop_duplicates(subset=['UID'])

    logger.debug('saving to {}'.format(args.output))
    sdf_papers.write.parquet(args.output)

if __name__ == "__main__":
    total_start = timer()
    logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("--papers", nargs='+', required=True, help="input files with paper data (TSV with header)")
    parser.add_argument("--clusters", required=True, help="file with cluster/flow data (TSV with header)")
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
