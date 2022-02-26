import gzip
import pandas as pd

path = 'E:\\SynologyDrive\\RickUbuntu\\JasonTest\\DataSources\\reviews_Movies_and_TV_5.json.gz'


# "parse("path") class is to read the large file of ".gz". We need to read the file row by row or column by cloumn
# path = 'reviews_Movies_and_TV_5.json.gz'
def parse(path):
  g = gzip.open(path, 'r')  # 'r' means
  for l in g:
    yield eval(l)
    # [Python] Generator 特性及實作場景
    # https://j-sui.com/2020/05/13/python-generator/
    # Python eval 用法及代碼示例
    # https://vimsky.com/zh-tw/examples/usage/python-programming_methods_built-in_eval.html
    # https://www.itread01.com/content/1515241329.html


def getDF(path):
  i = 0
  df = {}
  for d in parse(path):
    df[i] = d
    i += 1
    df_1 = pd.DataFrame.from_dict(df, orient='index')
    print(df_1.columns)
  return pd.DataFrame.from_dict(df, orient='index')


df = getDF(path)
