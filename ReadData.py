import gzip
import sys

import pandas as pd

#--------------------------------------------------------#
# Set path of the data 'reviews_Movies_and_TV_5.json.gz'
#--------------------------------------------------------#
# path = 'E:\\SynologyDrive\\RickUbuntu\\JasonTest\\DataSources\\reviews_Movies_and_TV_5.json.gz'
path = 'D:\\SynologyDrive\\RickUbuntu\\JasonTest\\DataSources\\reviews_Movies_and_TV_5.json.gz'

#------------------------------------------------------------------------------------
# parse("path") class is to read the large file of ".gz". We need to read the file
# row by row or column by column.
# Read the data row by row .
#------------------------------------------------------------------------------------
def parse(path):
    g = gzip.open(path, 'r')  # 'r' means
    for l in g:
        yield eval(l)
        # [Python] Generator 特性及實作場景
        # https://j-sui.com/2020/05/13/python-generator/
        # Python eval 用法及代碼示例
        # https://vimsky.com/zh-tw/examples/usage/python-programming_methods_built-in_eval.html
        # https://www.itread01.com/content/1515241329.html


#--------------------------------------
# Call parse and convert to DataFrame
#--------------------------------------
def getDF(path):
    i = 0
    df = {}
    for d in parse(path):
        df[i] = d
        i += 1
        df_row = pd.DataFrame.from_dict(df, orient='index')
        # Collect the first 10 data and save to output_10.cvs
        # In general, we will collect the short version of the data set from the large data set
        # before we perform this program for the the large data set.
        if i <= 10:
            # print(df_row) # print df_row
            df_row.to_csv("outdata_10.cvs", header=1, index=1)
        else:
            # operate the data set to know the format of data set.
            print(df_row.iloc[1, :]) # print the data of the first row.
            print(df_row.iloc[:, -1]) # print the data od the last column.
            print(df_row.columns) # print the Index
            print(df_row.loc[:, 'reviewTime']) # print data of reviewTime after we know the index.
            sys.exit("program is closed") # stop the program
    return pd.DataFrame.from_dict(df, orient='index')

#--------------------------
# Main program
#--------------------------
if __name__ == '__main__':
    df = getDF(path)

