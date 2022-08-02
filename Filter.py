from pandas import DataFrame
import copy


class Filter:
    @staticmethod
    def filter_yingjie(df: DataFrame, include: bool):
        copy_df = copy.deepcopy(df)
        for index, row in copy_df.iterrows():
            sf = row["现有身份要求"]
            if isinstance(copy_df.loc[index, "现有身份要求"], float):
                if include:
                    copy_df.drop(index, inplace=True)
                continue
            if (not include) and ("应届" in sf):
                copy_df.drop(index, inplace=True)
                continue
            elif include and ("应届" not in sf):
                copy_df.drop(index, inplace=True)
                continue
        return copy_df
