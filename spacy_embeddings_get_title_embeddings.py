# -*- coding: utf-8 -*-

DESCRIPTION = """get glove embeddings for paper titles. save as pickled pandas Series with index paper ID"""

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
import pyarrow.parquet as pq
import spacy

def main(args):
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError('input path {} does not exist'.format(input_path))
    outdir = Path(args.outdir)
    if not outdir.is_dir():
        raise FileNotFoundError('output directory {} does not exist'.format(outdir))
    id_colname = args.id_colname
    title_colname = args.title_colname

    language_model_name = "en_core_web_lg"
    disabled_components = ['parser', 'tagger']
    logger.debug("loading spacy language model: {}. disabling components: {}".format(language_model_name, disabled_components))
    nlp = spacy.load(language_model_name, disable=disabled_components)

    logger.debug("loading input data from {} (piece index: {})".format(input_path, args.idx))
    parquet_dataset = pq.ParquetDataset(input_path)
    piece = parquet_dataset.pieces[args.idx]
    df = piece.read(columns=[id_colname, title_colname]).to_pandas()
    df = df.set_index(id_colname)
    logger.debug("Loaded {} rows".format(len(df)))

    logger.debug("finding glove embeddings for column {}".format(title_colname))
    start = timer()
    data = []
    for doc in nlp.pipe(df[title_colname].values, n_process=-1):
        data.append(doc.vector)
    embeddings = pd.Series(data, index=df.index)
    logger.debug("finding embeddings took {}".format(format_timespan(timer()-start)))

    outfpath = outdir.joinpath('embeddings_{:05d}.pickle'.format(args.idx))
    logger.debug("saving to pickle file: {}".format(outfpath))
    embeddings.to_pickle(outfpath)

if __name__ == "__main__":
    total_start = timer()
    # logger = logging.getLogger(__name__)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    import argparse
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("input", help="input parquet directory")
    parser.add_argument("idx", type=int, help="which parquet piece to process (integer from 0 to len(pieces)-1)")
    parser.add_argument("outdir", help="output directory (should exist already. the output pickle file will be named automatically and saved here)")
    parser.add_argument("--id-colname", default='UID', help="column name for paper id (default: \"UID\")")
    parser.add_argument("--title-colname", default='title', help="column name for title (default: \"title\")")
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
#
##
