#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import boto3
import traceback
import pandas as pd
import catboost as ctb
from urllib.parse import urlparse
from tempfile import TemporaryFile
from pyarrow import parquet as pq, fs


def s3_blob(f):
    def inner(*args, **kwargs):
        return {'type': 's3-blob', 'data': {'uri': f(*args, **kwargs)}}
    return inner


def read_s3_json(path):
    path = urlparse(path)
    obj = boto3.client('s3').get_object(Bucket=path.netloc, Key=path.path[1:])
    return json.loads(obj['Body'].read().decode())


@s3_blob
def write_s3_json(js, path):
    js = json.dumps(js, sort_keys=True, separators=(',', ':'))
    path = urlparse(path)
    boto3.client('s3').put_object(Bucket=path.netloc, Key=path.path[1:],
                                  Body=js.encode())
    return path


@s3_blob
def to_parquet(df, loc, **kwargs):
    df.to_parquet(f'{loc}/data.parquet', **kwargs)
    return loc


def get_data(loc, index_cols=None):
    loc = urlparse(loc)
    file_system = None
    if loc.scheme == 's3':
        loc = loc.netloc + loc.path
        file_system = fs.S3FileSystem()
    else:
        loc = loc.path
    df = pq.ParquetDataset(loc, filesystem=file_system)\
        .read_pandas().to_pandas()
    if index_cols is not None:
        df = df.set_index(index_cols)
    return df


def fit(data_loc, model_params, data_params, predict_params, output_loc):
    train = get_data(data_loc, data_params['index_cols'])
    X = train.drop(data_params['target_col'], axis=1)
    y = train[data_params['target_col']]
    model = ctb.CatBoostRegressor(**model_params)
    model.fit(X, y)
    outputs = {}
    with TemporaryFile(mode='w') as f:
        model.save_model(f.name, format='json')
        outputs['model'] = write_s3_json(json.loads(f.read()),
                                         output_loc + 'model/artifact.json')
    preds = pd.Series(model.predict(X, **predict_params), index=X.index)
    outputs['preds'] = to_parquet(preds.to_frame('prediction').reset_index(),
                                  output_loc + 'train/preds.parquet',
                                  index=False)
    print(json.dumps(outputs, indent=2))
    return {'type': 'map', 'data': outputs}


def predict(data_loc, model_loc, data_params, predict_params, output_loc):
    X = get_data(data_loc, data_params['index_cols'])
    model = ctb.CatBoost().load_model(model_loc, format='json')
    preds = pd.Series(model.predict(X, **predict_params), index=X.index)
    return to_parquet(
        preds.to_frame('prediction').reset_index(),
        output_loc + 'predictions/data.parquet',
        index=False
    )


FUNCS = {
    'fit': fit,
    'predict': predict
}


def main(inputs, output_loc, output_manifest_loc):
    if not output_loc.endswith('/'):
        output_loc += '/'
    inputs = iter(inputs)
    kind = next(inputs)
    datum = FUNCS[kind](*inputs, output_loc)
    write_s3_json(datum, output_manifest_loc)
    return True


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
