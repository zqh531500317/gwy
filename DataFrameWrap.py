from pandas import DataFrame
import copy


class DataFrameWrap:
    def __init__(self, df: DataFrame):
        self.df = df
        self.descList = list()

    def set_df(self, df: DataFrame, desc: str):
        df_copy = copy.deepcopy(df)
        new_dw = DataFrameWrap(df_copy)
        new_dw.descList = copy.deepcopy(self.descList)
        new_dw.descList.append(desc)
        return new_dw

    def __str__(self):
        temp = "限制条件==>"
        index = 1
        for desc in self.descList:
            temp += "{}、{}  ".format(index, desc)
            index += 1
        return temp
