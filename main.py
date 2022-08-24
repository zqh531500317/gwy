import math
import os.path
import re
import sqlite3
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine

from DataFrameWrap import DataFrameWrap
from Filter import Filter
import unittest


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
        zhuanyevalue = [0] * len(self.zhuanyekey)

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

    def func_zj_huizong(self, name):
        df = pd.read_excel(name, 0)
        return df


class DataProcess:
    def __init__(self, year, reload=True):
        self.data = Data()
        if reload or not os.path.exists("{}-result.xlsx".format(year)):
            self.creatxlsx(year)

    def creatxlsx(self, year):
        print("=====reload 2022-test.xlsx=======")
        # 读取 职位表和缴费情况表
        df = self.data.func_zj_zwb(year)
        self.data.get_zhuanye_zwb_num(df)
        jfqk_zj = self.data.func_zj_jfqk(year)
        # 按照职务代码合并
        res = pd.concat([df, jfqk_zj], axis=0).groupby('职位代码').first().reset_index()
        # 计算并新增‘竞争比’列
        res["竞争比"] = res["缴费人数"] / res["招录人数"]
        # 读取各县市 成绩信息表
        match_file = []
        pattern_str = '.*' + '.*'.join(["{}-zjsk(.*?)-jmmd(.*?)".format(year)])
        re_pattern = re.compile(pattern=pattern_str)
        file_list = os.listdir(".")
        for file_name in file_list:
            if re_pattern.search(file_name):
                match_file.append(file_name)
        ...
        # 合并 计算max min
        dflist = []
        for path in match_file:
            dflist.append(self.data.func_zj_jmmd(path))
        df = pd.concat(dflist)
        # 按照 '招录单位名称','职位名称' 合并 成绩信息表 和 res表
        r = pd.merge(res, df, on=['招录单位名称', '职位名称'], how='left')

        # 导出表
        self.data.write(r, "{}-result.xlsx".format(year))

        # 生成数据库
        self.xlsx2sql(year, df=r)

    def xlsx2sql(self, year, name=None, df=None):
        if df is None:
            if not os.path.exists(name):
                raise Exception("xlsx2sql 文件不存在")
            df = pd.read_excel(name, 0)
        df.loc[:'year'] = year
        conn = sqlite3.connect('main.db')
        conn.commit()
        df.to_sql('main', conn, if_exists='replace', index=False)

    def single_cal(self, dw: DataFrameWrap):
        temp = [0, 0, 0, 0, 0, 0, 0]
        df = dw.df
        for index, row in df.iterrows():
            if math.isnan(row["max"]) or \
                    math.isnan(row["min"]):
                continue
            temp[0] += row["招录人数"]
            temp[1] += row["缴费人数"]
            temp[2] += row["max"] * row["招录人数"]
            temp[3] += row["min"] * row["招录人数"]
        # 计算
        if temp[0] == 0:
            temp[4] = temp[5] = temp[6] = 0
        else:
            temp[4] = temp[2] / temp[0]
            temp[5] = temp[3] / temp[0]
            temp[6] = temp[1] / temp[0]
        print("==={}============".format(dw))
        print("岗位数{},最高平均分{},进面平均分{},竞争比{}".format(temp[0], temp[4], temp[5], temp[6]))

    # hujilimit 限户籍 0不考虑 1筛选限制的 2排除限制的
    def cal(self, yingjie=False, area="00", hujilimit=0, zhuanyelimit=True):
        # key:专业 value:[职位数,总人数,岗位最高总分,岗位最低总分,最高平均分,最低平均分,竞争比]
        zhuanyemap = dict()
        df = self.data.func_zj_huizong("2021-test.xlsx")
        # 过滤 去除公安局警察
        df = df[df['max'] >= 100]
        # 地区过滤 职位代码133XY001003000000
        # XY=>01:杭州 02:宁波 03:温州 04:嘉兴 05:湖州
        # 06：绍兴 07:金华 08:衢州 09:舟山 10:台州
        # 11:丽水 12 省直 13: 省直公安 14:监狱
        if area != "00":
            areacodemap = {}
            areacodemap["01"] = "杭州"
            areacodemap["02"] = "宁波"
            areacodemap["03"] = "温州"
            areacodemap["04"] = "嘉兴"
            areacodemap["05"] = "湖州"
            areacodemap["06"] = "绍兴"
            start = int("133{}000000000000".format(area))
            end = int("133{}999999999999".format(area))
            df = df[df.职位代码.between(start, end)]
            for x in df.index:
                if isinstance(df.loc[x, "备注"], float):
                    if hujilimit == 1:
                        df.drop(x, inplace=True)
                    continue
                if areacodemap[area] in df.loc[x, "备注"] or "本市" in df.loc[x, "备注"] or "户籍" in df.loc[x, "备注"] or "生源" in \
                        df.loc[x, "备注"]:
                    if hujilimit == 2:
                        df.drop(x, inplace=True)
                else:
                    if hujilimit == 1:
                        df.drop(x, inplace=True)
        c = "不限户籍"
        if hujilimit == 1:
            c = "仅限户籍"
        elif hujilimit == 2:
            c = "排除户籍限制"
        print("======={},{}地区==========".format(c, area))
        if zhuanyelimit:
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
                print("专业:{} 岗位数{},最高平均分{},进面平均分{},竞争比{}".format(k, val[0], val[4], val[5], val[6]))

        else:
            temp = [0, 0, 0, 0, 0, 0, 0]
            for index, row in df.iterrows():
                if math.isnan(row["max"]) or \
                        math.isnan(row["min"]):
                    continue
                temp[0] += row["招录人数"]
                temp[1] += row["缴费人数"]
                temp[2] += row["max"] * row["招录人数"]
                temp[3] += row["min"] * row["招录人数"]
            # 计算
            temp[4] = temp[2] / temp[0]
            temp[5] = temp[3] / temp[0]
            temp[6] = temp[1] / temp[0]

            print("岗位数{},最高平均分{},进面平均分{},竞争比{}".format(temp[0], temp[4], temp[5], temp[6]))


# http://www.zyksw.cn/articles?navLv1Id=1&navLv2Id=3&sectionKey=lv3_provincial_exam_6&subjectId=&page=20
if __name__ == '__main__':
    print('''
          00:汇总 01:杭州 02:宁波 03:温州 04:嘉兴 05:湖州
          06：绍兴 07:金华 08:衢州 09:舟山 10:台州
          11:丽水 12 省直 13: 省直公安 14:监狱
          ''')
    year = 2022
    p = DataProcess(reload=False, year=year)
    # p.cal(yingjie=True)
    # p.cal(yingjie=True, area="01", hujilimit=1, zhuanyelimit=False)
    # p.cal(yingjie=True, area="01", hujilimit=2, zhuanyelimit=False)
    # p.cal(yingjie=True, area="02", hujilimit=1, zhuanyelimit=False)
    # p.cal(yingjie=True, area="02", hujilimit=2, zhuanyelimit=False)
    # p.cal(yingjie=True, area="03", hujilimit=1, zhuanyelimit=False)
    # p.cal(yingjie=True, area="03", hujilimit=2, zhuanyelimit=False)
    # p.cal(yingjie=True, area="06", hujilimit=1, zhuanyelimit=False)
    # p.cal(yingjie=True, area="06", hujilimit=2, zhuanyelimit=False)
    df = p.data.func_zj_huizong(name="{}-test.xlsx".format(year))
    # 过滤 去除公安局警察
    df = df[df['max'] >= 100]
    dw = DataFrameWrap(df)
    yingjie = [False]
    sex = ["男"]
    sexbuxian = [True, False]
    zhuanye = ["语言", "法学",  "财务", "林", "农"]
    xueli = [False]
    for x in yingjie:
        for y in sex:
            for z in zhuanye:
                for w in xueli:
                    for zz in sexbuxian:
                        dwx = Filter.filter_yingjie(dw, x)
                        dwy = Filter.filter_sex(dwx, y, include_buxian=zz)
                        dwz = Filter.filter_zhuanye(dwy, z)
                        dww = Filter.filter_xueli(dwz, "硕士研究生及以上", include=w)
                        dwv = Filter.filter_special(dww, "备注", "司法", include=False)
                        dwv = Filter.filter_special(dwv, "备注", "杭州", include=False)
                        dwv = Filter.filter_special(dwv, "备注", "淳安", include=False)
                        dwv = Filter.filter_special(dwv, "备注", "宁波", include=False)
                        p.single_cal(dwv)
