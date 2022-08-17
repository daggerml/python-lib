#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import boto3
import traceback
import numpy as np
import pandas as pd
from urllib.parse import urlparse


def read_s3_json(path):
    path = urlparse(path)
    obj = boto3.client('s3').get_object(Bucket=path.netloc, Key=path.path[1:])
    return json.loads(obj['Body'].read().decode())


def write_s3_json(js, path):
    js = json.dumps(js, sort_keys=True, separators=(',', ':'))
    path = urlparse(path)
    boto3.client('s3').put_object(Bucket=path.netloc, Key=path.path[1:],
                                  Body=js.encode())
    return


def to_parquet(df, loc, **kwargs):
    df.to_parquet(loc, **kwargs)
    return loc


def mkdata(rng, shape):
    data = rng.normal(size=shape)
    df = pd.DataFrame(data,
                      columns=[f'x{i}' for i in range(data.shape[1])])
    return df


def main(inputs, output_loc, out_manifest_loc):
    output_loc = output_loc.rstrip('/')
    seed, num_train_rows, num_test_rows, num_ids = [x for x in inputs]
    rng = np.random.default_rng(seed)
    d0 = to_parquet(mkdata(rng, (num_train_rows, num_ids)),
                    f'{output_loc}/train/data.snappy.parquet', index=False)
    d1 = to_parquet(mkdata(rng, (num_test_rows, num_ids)),
                    f'{output_loc}/test/data.snappy.parquet', index=False)
    write_s3_json({'train': d0, 'test': d1},
                  out_manifest_loc)
    return


if __name__ == '__main__':
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(description='Process some integers.',
                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('manifest', type=str,
                        help='file path of the io-manifest.')
    args = parser.parse_args()
    manifest = read_s3_json(args.manifest)
    print('manifest ==', json.dumps(manifest))
    try:
        exit(main(manifest['args'], manifest['output'],
                  manifest['output-manifest']))
    except Exception:
        print('an exception was raised during training.')
        print('========================================')
        print(traceback.format_exc())
        print('========================================')
        raise
