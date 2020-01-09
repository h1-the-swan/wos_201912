# -*- coding: utf-8 -*-

DESCRIPTION = """filter citations to only include papers for which we have clustering information. Uses spark"""

import sys, os, time
from pathlib import Path
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

import pandas as pd
import numpy as np

from config import Config

def main(args):
    config = Config()
    spark = config.spark

    id_colname = args.id_colname

    try:
        logger.debug("loading spark dataframe from {}".format(args.citations))
        sdf_citations = spark.read.parquet(args.citations)
        logger.debug("citations dataframe has {} rows".format(sdf_citations.count()))

        logger.debug("loading spark dataframe from {}".format(args.papers))
        sdf_papers = spark.read.parquet(args.papers)
        logger.debug("papers dataframe has {} rows".format(sdf_papers.count()))
        logger.debug("dropping rows with no cluster information")
        sdf_papers = sdf_papers.dropna(subset=['cl'])
        sdf_papers.persist()
        logger.debug("after dropping rows, papers dataframe has {} rows".format(sdf_papers.count()))

        logger.debug("joining citations dataframe with papers dataframe IDs on both columns, to filter out dropped rows")
        sdf_papers_ids = sdf_papers.select([args.id_colname])
        sdf_citations = sdf_citations.join(sdf_papers_ids, on=args.id_colname, how='inner')
        sdf_citations = sdf_citations.join(sdf_papers_ids.withColumnRenamed(args.id_colname, args.cited_colname), on=args.cited_colname, how='inner')
        sdf_citations.persist()
        logger.debug("citations dataframe now has {} rows".format(sdf_citations.count()))

        logger.debug("saving citations to {}".format(args.output))
        sdf_citations.write.parquet(args.output)

    finally:
        config.teardown()

if __name__ == "__main__":
    total_start = timer()
    # logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("citations", help="path to original citations data, in parquet format")
    parser.add_argument("output", help="path to output (parquet)")
    parser.add_argument("--papers", help="path to papers data, with a 'cl' column.")
    parser.add_argument("--id-colname", default='UID', help="column name for paper id (default: \"UID\")")
    parser.add_argument("--cited-colname", default='cited_UID', help="column name for cited paper id (default: \"cited_UID\")")
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
