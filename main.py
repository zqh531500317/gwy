import pandas as pd


def func_2021_zj_zwb():
    ptgw = pd.read_excel(r'2021-zj-zwb.xlsx', 0)
    print(ptgw.shape)
    xzzfl = pd.read_excel(r'2021-zj-zwb.xlsx', 1)
    print(xzzfl.shape)
    zyjsl = pd.read_excel(r'2021-zj-zwb.xlsx', 2)
    print(zyjsl.shape)
    res = pd.concat([ptgw, xzzfl, zyjsl])
    print(func_2021_zj_zwb, res.shape)
    return res


def get_zhuanye_zwb_num(df):
    zhuanyekey = ["计算机科学与技术", "法学", "汉语言"]
    zhuanyevalue = [0, 0, 0]

    for index, row in df.iterrows():
        zhuanye = row[16]
        for num in range(len(zhuanyekey)):
            if zhuanyekey[num] in zhuanye:
                zhuanyevalue[num] += 1

    print(zhuanyekey)
    print(zhuanyevalue)


# http://www.zyksw.cn/articles?navLv1Id=1&navLv2Id=3&sectionKey=lv3_provincial_exam_6&subjectId=&page=20
if __name__ == '__main__':
    res = func_2021_zj_zwb()
    get_zhuanye_zwb_num(res)
