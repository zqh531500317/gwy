import copy

from DataFrameWrap import DataFrameWrap


class Filter:
    @staticmethod
    def filter_yingjie(dw: DataFrameWrap, include: bool):
        df = dw.df
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
        if include:
            desc = "仅限应届"
        else:
            desc = "排除应届"
        dw = dw.set_df(copy_df, desc)

        return dw

    @staticmethod
    def filter_zhuanye(dw: DataFrameWrap, name: str):
        df = dw.df
        copy_df = copy.deepcopy(df)
        for index, row in copy_df.iterrows():
            zy = row["专业要求"]
            if isinstance(copy_df.loc[index, "专业要求"], float):
                copy_df.drop(index, inplace=True)
                continue
            if name not in zy:
                copy_df.drop(index, inplace=True)
                continue
        desc = "限制{}专业".format(name)
        dw = dw.set_df(copy_df, desc)

        return dw

    @staticmethod
    def filter_sex(dw: DataFrameWrap, sex: str, include_buxian: bool):
        df = dw.df
        copy_df = copy.deepcopy(df)
        for index, row in copy_df.iterrows():
            xb = row["性别要求"]
            if isinstance(copy_df.loc[index, "性别要求"], float):
                copy_df.drop(index, inplace=True)
                continue
            if include_buxian and xb == "不限":
                continue
            if sex not in xb:
                copy_df.drop(index, inplace=True)
                continue
        desc = "限制{}".format(sex)
        if include_buxian:
            desc += "及不限制"
        dw = dw.set_df(copy_df, desc)
        return dw

    @staticmethod
    def filter_huji(dw: DataFrameWrap, area: str, include: bool):
        df = dw.df
        copy_df = copy.deepcopy(df)
        areacodemap = {}
        areacodemap["01"] = "杭州"
        areacodemap["02"] = "宁波"
        areacodemap["03"] = "温州"
        areacodemap["04"] = "嘉兴"
        areacodemap["05"] = "湖州"
        areacodemap["06"] = "绍兴"
        start = int("133{}000000000000".format(area))
        end = int("133{}999999999999".format(area))
        copy_df = copy_df[copy_df.职位代码.between(start, end)]
        for x in copy_df.index:
            if isinstance(copy_df.loc[x, "备注"], float):
                if include:
                    copy_df.drop(x, inplace=True)
                continue
            if areacodemap[area] in copy_df.loc[x, "备注"] or "本市" in copy_df.loc[x, "备注"] or "户籍" in copy_df.loc[
                x, "备注"] or "生源" in \
                    copy_df.loc[x, "备注"]:
                if not include:
                    copy_df.drop(x, inplace=True)
            else:
                if include:
                    copy_df.drop(x, inplace=True)
        if include:
            desc = "户籍仅限{}".format(areacodemap[area])
        else:
            desc = "户籍排除{}".format(areacodemap[area])
        dw = dw.set_df(copy_df, desc)

        return dw

    @staticmethod
    def filter_area(dw: DataFrameWrap, area: str):
        if area == "00":
            return dw
        elif area != "00":
            df = dw.df
            copy_df = copy.deepcopy(df)
            areacodemap = {}
            areacodemap["01"] = "杭州"
            areacodemap["02"] = "宁波"
            areacodemap["03"] = "温州"
            areacodemap["04"] = "嘉兴"
            areacodemap["05"] = "湖州"
            areacodemap["06"] = "绍兴"
            start = int("133{}000000000000".format(area))
            end = int("133{}999999999999".format(area))
            copy_df = copy_df[copy_df.职位代码.between(start, end)]
            desc = "筛选区域{}".format(areacodemap[area])
            dw = dw.set_df(copy_df, desc)
            return dw

    @staticmethod
    def filter_xueli(dw: DataFrameWrap, xueli: str, include: bool):
        if (xueli == "" or xueli is None) and include:
            return dw
        df = dw.df
        copy_df = copy.deepcopy(df)
        for index, row in copy_df.iterrows():
            xb = row["学历要求"]
            if isinstance(copy_df.loc[index, "学历要求"], float):
                copy_df.drop(index, inplace=True)
                continue
            if xueli not in xb and include:
                copy_df.drop(index, inplace=True)
                continue
            if xueli in xb and not include:
                copy_df.drop(index, inplace=True)
                continue
        if include:
            str1 = "限制"
        else:
            str1 = "排除"
        desc = "{}{}".format(str1, xueli)
        dw = dw.set_df(copy_df, desc)
        return dw

    @staticmethod
    def filter_special(dw: DataFrameWrap, key, valueexcept, include, func=None):
        df = dw.df
        copy_df = copy.deepcopy(df)
        for index, row in copy_df.iterrows():
            xb = row[key]
            if isinstance(copy_df.loc[index, key], float):
                copy_df.drop(index, inplace=True)
                continue
            if func is None:
                if valueexcept not in xb and include:
                    copy_df.drop(index, inplace=True)
                    continue
                if valueexcept in xb and not include:
                    copy_df.drop(index, inplace=True)
                    continue
            else:
                if not func(xb, valueexcept) and include:
                    copy_df.drop(index, inplace=True)
                    continue
                if func(xb, valueexcept) and not include:
                    copy_df.drop(index, inplace=True)
                    continue
        if include:
            str1 = "限制"
        else:
            str1 = "排除"
        desc = "{}-{}-{}".format(str1, key, valueexcept)
        dw = dw.set_df(copy_df, desc)
        return dw
