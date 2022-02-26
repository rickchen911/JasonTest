import json
import gzip

path = 'E:\\SynologyDrive\\RickUbuntu\\JasonTest\\DataSources\\reviews_Movies_and_TV_5.json.gz'


def parse(path):
  g = gzip.open(path, 'r')
  for l in g:
    yield json.dumps(eval(l))


if __name__ == '__main__':
  f = open("output.strict", 'w')
  for l in parse(path):
    f.write(l + '\n')
