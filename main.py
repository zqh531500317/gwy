import os.path

import numpy as np
import pandas as pd


class Data:
    def __init__(self):
        self.zhuanyekey = ["计算机科学与技术", "法学", "汉语言"]

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

    def func_zj_jmmd(self, year, city):
        print(year, city)
        if os.path.exists('{}-zjsk-{}-jmmd.xlsx'.format(year, city)):
            name = '{}-zjsk-{}-jmmd.xlsx'.format(year, city)
        else:
            name = '{}-zjsk-{}-jmmd.xls'.format(year, city)
        df = pd.read_excel(name, 0)
        if {'招考单位'}.issubset(df.columns):
            temp1 = "招考单位"
        elif {"招考单位名称"}.issubset(df.columns):
            temp1 = "招考单位名称"
        elif {"报考单位名称"}.issubset(df.columns):
            temp1 = "招考单位名称"
        elif {"报考单位"}.issubset(df.columns):
            temp1 = "招考单位"
        else:
            raise Exception(df.columns.values)
        if {"职位名称"}.issubset(df.columns):
            temp2 = "职位名称"
        elif {"报考职位名称"}.issubset(df.columns):
            temp2 = "报考职位名称"
        elif {"报考职位"}.issubset(df.columns):
            temp2 = "报考职位"
        else:
            raise Exception(df.columns.values)
        df.rename(columns={'报考单位名称': '招录单位名称', '招考单位名称': '招录单位名称', '招考单位': '招录单位名称', '报考单位': '招录单位名称'}, inplace=True)
        df.rename(columns={'报考职位名称': "职位名称", '报考职位': "职位名称"}, inplace=True)
        df = df.groupby(['招录单位名称', '职位名称'], as_index=False).agg({'总分': [max, min]})
        return df

    def write(self, df, name):
        writer = pd.ExcelWriter(name)  # 初始化一个writer
        df.to_excel(writer, float_format='%.5f', index=False)  # table输出为excel, 传入writer
        writer.save()  # 保存


# http://www.zyksw.cn/articles?navLv1Id=1&navLv2Id=3&sectionKey=lv3_provincial_exam_6&subjectId=&page=20
if __name__ == '__main__':
    data = Data()
    # 读取2021 职位表和缴费情况表
    df = data.func_zj_zwb(2021)
    data.get_zhuanye_zwb_num(df)
    jfqk_zj_2022 = data.func_zj_jfqk(2021)
    # 按照职务代码合并
    res = pd.concat([df, jfqk_zj_2022], axis=0).groupby('职位代码').first().reset_index()
    # 计算并新增‘竞争比’列
    res["竞争比"] = res["缴费人数"] / res["招录人数"]
    # 读取各县市2021 成绩信息表
    temp = ['shaoxin-keqiao', 'shaoxin-shangyu', 'shaoxin-shaoxin', 'shaoxin-shenzhou', 'shaoxin-xinchang',
            'shaoxin-zhuji']
    ...
    # 进行合并
    dflist = []
    for path in temp:
        dflist.append(data.func_zj_jmmd(2021, path))
    df = pd.concat(dflist)
    # 按照 '招录单位名称','职位名称' 合并 成绩信息表 和 res表
    res = pd.merge(res, df, on=['招录单位名称', '职位名称'], how='left')

    # 导出表
    data.write(res, "2021-result.xlsx")
