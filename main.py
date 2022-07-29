import math
import os.path
import re

import numpy as np
import pandas as pd
from pandas import DataFrame


class Data:
    def __init__(self):
        self.zhuanyekey = ["计算机科学与技术", "法学", "汉语言", "土木", "会计"]

    def func_zj_zwb(self, year):
        if os.path.exists('{}-zjsk-zwb.xlsx'.format(year)):
            name = '{}-zjsk-zwb.xlsx'.format(year)
        else:
            name = '{}-zjsk-zwb.xls'.format(year)
        ptgw = pd.read_excel(name, 0)
        xzzfl = pd.read_excel(name, 1)
        zyjsl = pd.read_excel(name, 2)
        res = pd.concat([ptgw, xzzfl, zyjsl])
        return res

    def get_zhuanye_zwb_num(self, df):
        zhuanyevalue = [0, 0, 0]

        for index, row in df.iterrows():
            zhuanye = row[16]
            for num in range(len(self.zhuanyekey)):
                if self.zhuanyekey[num] in zhuanye:
                    zhuanyevalue[num] += 1

        print(self.zhuanyekey)
        print(zhuanyevalue)

    def func_zj_jfqk(self, year):
        if os.path.exists('{}-zjsk-jfqk.xlsx'.format(year)):
            name = '{}-zjsk-jfqk.xlsx'.format(year)
        else:
            name = '{}-zjsk-jfqk.xls'.format(year)
        # '所属地市', '招录部门', '职位名称', '职位代码', '招录人数', '缴费人数'
        df = pd.read_excel(name, 0)
        return df

    def func_zj_jmmd(self, path):
        print("==={}===".format(path))
        df = pd.read_excel(path, 0)
        if {'招考单位'}.issubset(df.columns):
            temp1 = "招考单位"
        elif {"招考单位名称"}.issubset(df.columns):
            temp1 = "招考单位名称"
        elif {"报考单位名称"}.issubset(df.columns):
            temp1 = "招考单位名称"
        elif {"报考单位"}.issubset(df.columns):
            temp1 = "招考单位"
        elif {"招录部门"}.issubset(df.columns):
            temp1 = "招录部门"
        elif {"招录单位"}.issubset(df.columns):
            temp1 = "招录单位"
        else:
            print(path)
            raise Exception(df.columns.values)
        if {"职位名称"}.issubset(df.columns):
            temp2 = "职位名称"
        elif {"报考职位名称"}.issubset(df.columns):
            temp2 = "报考职位名称"
        elif {"报考职位"}.issubset(df.columns):
            temp2 = "报考职位"
        elif {"招考职位"}.issubset(df.columns):
            temp2 = "招考职位"
        elif {"招录职位"}.issubset(df.columns):
            temp2 = "招录职位"
        elif {"报考岗位"}.issubset(df.columns):
            temp2 = "报考岗位"
        else:
            print(path)
            raise Exception(df.columns.values)
        df.rename(columns={'报考单位名称': '招录单位名称',
                           '招考单位名称': '招录单位名称',
                           '招考单位': '招录单位名称',
                           '报考单位': '招录单位名称',
                           '招录单位': '招录单位名称',
                           '招录部门': '招录单位名称'
                           }, inplace=True)
        df.rename(columns={'报考职位名称': "职位名称",
                           '报考职位': "职位名称",
                           '招考职位': "职位名称",
                           '招录职位': "职位名称",
                           '报考岗位': "职位名称",
                           }, inplace=True)
        df = df.groupby(['招录单位名称', '职位名称'], as_index=False).agg({'总分': [max, min]})
        return df

    def write(self, df, name):
        writer = pd.ExcelWriter(name)  # 初始化一个writer
        df.to_excel(writer, float_format='%.5f', index=False)  # table输出为excel, 传入writer
        writer.save()  # 保存

    def func_zj_huizong(self, name="test.xlsx"):
        df = pd.read_excel(name, 0)
        return df


class DataProcess:
    def __init__(self, reload=True):
        self.data = Data()
        if reload or not os.path.exists("test.xlsx"):
            print("=====reload test.xlsx=======")
            # 读取2021 职位表和缴费情况表
            df = self.data.func_zj_zwb(2021)
            self.data.get_zhuanye_zwb_num(df)
            jfqk_zj_2022 = self.data.func_zj_jfqk(2021)
            # 按照职务代码合并
            res = pd.concat([df, jfqk_zj_2022], axis=0).groupby('职位代码').first().reset_index()
            # 计算并新增‘竞争比’列
            res["竞争比"] = res["缴费人数"] / res["招录人数"]
            # 读取各县市2021 成绩信息表
            match_file = []
            pattern_str = '.*' + '.*'.join(["{}-zjsk(.*?)-jmmd(.*?)".format(2021)])
            re_pattern = re.compile(pattern=pattern_str)
            file_list = os.listdir(".")
            for file_name in file_list:
                if re_pattern.search(file_name):
                    match_file.append(file_name)
            ...
            # 进行合并
            dflist = []
            for path in match_file:
                dflist.append(self.data.func_zj_jmmd(path))
            df = pd.concat(dflist)
            # 按照 '招录单位名称','职位名称' 合并 成绩信息表 和 res表
            res = pd.merge(res, df, on=['招录单位名称', '职位名称'], how='left')

            # 导出表
            self.data.write(res, "2021-result.xlsx")

    def cal(self, yingjie=False):
        # key:专业 value:[职位数,总人数,岗位最高总分,岗位最低总分,最高平均分,最低平均分,竞争比]
        zhuanyemap = dict()
        df = self.data.func_zj_huizong()
        for index, row in df.iterrows():
            for zhuanye in self.data.zhuanyekey:
                if row["专业要求"] is None or \
                        isinstance(row["专业要求"], float) or \
                        math.isnan(row["max"]) or \
                        math.isnan(row["min"]):
                    continue
                if zhuanyemap.get(zhuanye) is None:
                    zhuanyemap[zhuanye] = [0, 0, 0, 0, 0, 0, 0]
                    zhuanyemap[zhuanye + "应届"] = [0, 0, 0, 0, 0, 0, 0]
                    zhuanyemap[zhuanye + "非应届"] = [0, 0, 0, 0, 0, 0, 0]
                sf = row["现有身份要求"]
                if zhuanye in row["专业要求"]:
                    zhuanyemap[zhuanye][0] += row["招录人数"]
                    zhuanyemap[zhuanye][1] += row["缴费人数"]
                    zhuanyemap[zhuanye][2] += row["max"] * row["招录人数"]
                    zhuanyemap[zhuanye][3] += row["min"] * row["招录人数"]
                    if yingjie:
                        if "应届" in sf:
                            zhuanyemap[zhuanye + "应届"][0] += row["招录人数"]
                            zhuanyemap[zhuanye + "应届"][1] += row["缴费人数"]
                            zhuanyemap[zhuanye + "应届"][2] += row["max"] * row["招录人数"]
                            zhuanyemap[zhuanye + "应届"][3] += row["min"] * row["招录人数"]
                        else:
                            zhuanyemap[zhuanye + "非应届"][0] += row["招录人数"]
                            zhuanyemap[zhuanye + "非应届"][1] += row["缴费人数"]
                            zhuanyemap[zhuanye + "非应届"][2] += row["max"] * row["招录人数"]
                            zhuanyemap[zhuanye + "非应届"][3] += row["min"] * row["招录人数"]
                continue
        # 计算
        for k, val in zhuanyemap.items():
            val[4] = val[2] / val[0]
            val[5] = val[3] / val[0]
            val[6] = val[1] / val[0]
        for k, val in zhuanyemap.items():
            print()
            print("专业:{} 岗位数{},最高平均分{},最低平均分{},竞争比{}".format(k, val[0], val[4], val[5], val[6]))


# http://www.zyksw.cn/articles?navLv1Id=1&navLv2Id=3&sectionKey=lv3_provincial_exam_6&subjectId=&page=20
if __name__ == '__main__':
    p = DataProcess(reload=False)
    p.cal(yingjie=True)
