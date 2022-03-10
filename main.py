import argparse
import pandas as pd
import time

parser = argparse.ArgumentParser(description='Script so useful.')
parser.add_argument("--raw", type=str)
parser.add_argument("--masked", type=str)
parser.add_argument("--delimiter", type=str)

args = parser.parse_args()


def generateDataFrames(raw_path, masked_path, separator):
    """
    This function generates the dataframes given the input csv files.
    """
    raw_df = pd.read_csv(raw_path, sep=separator)
    masked_df = pd.read_csv(masked_path, sep=separator)
    return raw_df, masked_df


def checkConsistency(df1, df2):
    """
    This function returns a matrix showing True/False for unmasked/masked data between 2 datasets.
    This function returns comparison matrix.
    """
    masking_matrix = pd.DataFrame()
    print(f'The shape of the RAW dataset is {df1.shape}.\nThe shape of masked dataset is {df2.shape}.\n')
    if df1.shape == df2.shape:
        print(f'The datasets match.\n')
    else:
        print("The shapes don't match.\n")
        pass
    for rowIndex, row in df1.iterrows():  # iterate over rows
        for columnIndex, value in row.items():
            if pd.isnull(df1[columnIndex].iloc[rowIndex]):
                if pd.isnull(df2[columnIndex].iloc[rowIndex]):
                    masking_matrix.at[rowIndex, columnIndex] = True
                elif pd.notnull(df2[columnIndex].iloc[rowIndex]):
                    masking_matrix.at[rowIndex, columnIndex] = False
            elif pd.notnull(df1[columnIndex].iloc[rowIndex]):
                if df1.at[rowIndex, columnIndex] == df2.at[rowIndex, columnIndex]:
                    masking_matrix.at[rowIndex, columnIndex] = False
                elif df1.at[rowIndex, columnIndex] != df2.at[rowIndex, columnIndex]:
                    if str(df1.at[rowIndex, columnIndex]) in str(df2.at[rowIndex, columnIndex]):
                        masking_matrix.at[rowIndex, columnIndex] = False
                    elif pd.isnull(df2[columnIndex].iloc[rowIndex]):
                        masking_matrix.at[rowIndex, columnIndex] = False
                    else:
                        masking_matrix.at[rowIndex, columnIndex] = True
    print(masking_matrix)
    return masking_matrix


def trueFalseRatio(comp_df):
    """
    This function returns ratio of masked / unmasked for each column in a given dataset. Result is a % of correctly
    masked values in each column.

    This function takes comparison matrix as an argument and returns dictionary of shape: {'column_name1': ratio1, etc.}
    """
    masking_list = []
    raw_list = []
    ratio = {}
    for column in comp_df:
        masking_list.append((comp_df[column].values.sum()))
    for column in raw_df:
        raw_list.append(raw_df[column].count())
    for index in range(len(raw_list)):
        ratio[raw_df.columns[index]] = ((masking_list[index]) * 100 / raw_list[index]).round(2)
    return ratio


def stats(ratio):
    for key, value in ratio.items():
        if 0 < value < 75:
            print(f'[] WARNING!!! {key} has been masked in {value}% !!!WARNING!!!')
        elif 75 <= value < 100:
            print(f"[] CHECK! {key} has been masked in {value}%")
        elif value == 100:
            print(f"[] SUCCESS! {key} has been masked in {value}%")
        elif value == 0:
            print(f"[] IN SCOPE? {key} has been masked in {value}%")


if __name__ == "__main__":
    start_time = time.time()
    raw_path = args.raw
    masked_path = args.masked
    delimiter = args.delimiter
    raw_df, masked_df = generateDataFrames(raw_path, masked_path, delimiter)
    #print(raw_df)
    #print('\n')
    #print(masked_df)
    #print("\n")
    maskingMatrix = checkConsistency(raw_df, masked_df)
    stats(trueFalseRatio(maskingMatrix))
    maskingMatrix.to_csv('maskingMatrix.csv', sep=';')
    print("[] INFO Took %s seconds to process" % (time.time() - start_time))
