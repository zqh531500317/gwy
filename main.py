import xlwings as xw

if __name__ == '__main__':
    app = xw.App(visible=True, add_book=False)
    app.display_alerts = True  # 关闭一些提示信息，可以加快运行速度。 默认为 True。
    app.screen_updating = False  # 更新显示工作表的内容。默认为 True。关闭它也可以提升运行速度。
    wb = app.books.open(r'1.xlsx')
    sht = wb.sheets[0]
    # 获取 b3 中的值
    # b3 = sht.range((1, 1), (3, 3))
    # v = b3.value
    # print(v)
    nrow = sht.api.UsedRange.Rows.count
    ncol = sht.api.UsedRange.Columns.count
    print(nrow)
    print(ncol)
    wb.close()
    app.quit()
