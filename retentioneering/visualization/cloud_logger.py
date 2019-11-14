# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


from matplotlib import pyplot as plt
import matplotlib.image as mpimg
from pymongo import MongoClient
import gridfs
from IPython.display import IFrame, display
import json


class MongoLoader(object):
    def __init__(self, client, db='retelogs', collection=None):
        self.client = MongoClient(client)
        self.db = self.client[db]
        self.collection = collection or 'pictures'
        self.fs = gridfs.GridFS(self.db, collection)

    def put(self, path, idx):
        with open(path, 'rb') as f:
            self.fs.put(f, filename=idx, fmt=path.split('.')[-1])
        self.client.close()

    def get(self, idx, path=None):
        x = None
        for i in self.fs.find({'filename': idx}):
            x = i.read()
            fmt = i.fmt
        if x is None:
            raise ValueError(f'There is no id in db: {idx}')
        with open((path or '') + idx + '.' + fmt, 'wb') as f:
            f.write(x)
        print(f'Your file saved at {(path or "") + idx + "." + fmt}')
        if fmt == 'png':
            img = mpimg.imread((path or '') + idx + '.' + fmt)
            plt.figure(figsize=[15, 8])
            plt.imshow(img)
            plt.show()
            plt.close()
        elif fmt == 'html':
            html = x.decode()
            width = int(html.split('var width = ')[1].split(',')[0])
            height = int(html.split('height = ')[1].split(';')[0])
            display(IFrame((path or '') + idx + '.' + fmt, width=width + 200, height=height + 200))
        elif fmt == 'json':
            with open((path or '') + idx + '.' + fmt) as f:
                x = json.load(f)
            return x
