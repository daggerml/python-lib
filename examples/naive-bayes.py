import os
import io
import numpy
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB

import daggerml as dml
from daggerml.executor import Sh

_here_ = os.path.dirname(__file__)

def dataFrameFromDirectory(path, classification):
    def readFiles(path):
        for filename in os.listdir(path):
            fname = os.path.join(path, filename)
            inBody = False
            lines = []
            with open(fname, 'r', encoding='latin1') as f:
                for line in f:
                    if inBody:
                        lines.append(line)
                    elif line == '\n':
                        inBody = True
            message = '\n'.join(lines)
            yield path, message
    rows = []
    for filename, message in readFiles(path):
        rows.append({'message': message, 'class': classification})
    return serialize(pd.DataFrame(rows), index=False)


def serialize(df, index):
    import io
    sio = io.BytesIO()
    df.to_csv(sio, index=index)
    return sio.getvalue()


def deserialize(df_str):
    df = pd.read_csv(df_str)
    return df


def union_df(*rsrcs):
    import io
    import pandas as pd
    from daggerml.executor import Sh
    dflist = []
    for rsrc in rsrcs:
        with Sh(None).get_file(rsrc) as f:
            dflist.append(pd.read_csv(f))
    df = pd.concat(dflist)
    sio = io.BytesIO()
    df.to_csv(sio, index=False)
    return Sh(None)._put_bytes_(sio.getvalue())


if __name__ == '__main__':
    print(f'{_here_ = }')
    dag = dml.Dag.new('foopy2', 'doopy')
    # spams = Sh(dag).put_bytes_(
    #     dataFrameFromDirectory(f"{_here_}/naive-bayes-data/spam", "spam")
    # )
    # hams = Sh(dag).put_bytes_(
    #     dataFrameFromDirectory(f"{_here_}/naive-bayes-data/ham", "ham")
    # )
    # dag.commit([spams, hams])
    n0 = dag.load('foopy')
    spams, hams = (dag.put(x) for x in dag.get_value(n0))
    data = Sh(dag).run(union_df, spams, hams)
    print(f'{dag.get_value(data) = }')

    ##################
    vectorizer = CountVectorizer()
    counts = vectorizer.fit_transform(data['message'].values)

    classifier = MultinomialNB()
    targets = data['class'].values
    classifier.fit(counts, targets)

    #################
    examples = ['Free Viagra now!!!', "Hi Bob, how about a game of golf tomorrow?"]
    example_counts = vectorizer.transform(examples)
    predictions = classifier.predict(example_counts)
    predictions
