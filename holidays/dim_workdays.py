"""
# 依赖项，每年更新一次即可
`pip install chinesecalendar -U`

字段：
    日期
    年份
    月份
    日
    周数
    星期（1-7表示周一至周天）
    日期类型（工作日，周末，法定上班，节假日名称）
    是否工作日（1表示工作日，0表示非工作日）

date,uyear,umonth,uday,week,weekday,date_type,is_workday
2023-01-01,2023,1,1,00,7,元旦节,0
2023-01-02,2023,1,2,01,1,元旦节,0
2023-01-03,2023,1,3,01,2,工作日,1
2023-01-04,2023,1,4,01,3,工作日,1
2023-01-05,2023,1,5,01,4,工作日,1
2023-01-06,2023,1,6,01,5,工作日,1
2023-01-07,2023,1,7,01,6,周末,0
"""
import datetime
import csv
import chinese_calendar

# 起始日期
start_date = datetime.date(2023, 1, 1)
# 结束日期：默认本年最后一天
end_date = datetime.date(datetime.date.today().year, 12, 31)

holidays_name_dict = {
    "New Year's Day": '元旦节',
    "Spring Festival": '春节',
    "Tomb-sweeping Day": '清明节',
    "Labour Day": '劳动节',
    "Dragon Boat Festival": '端午节',
    "Mid-autumn Festival": '中秋节',
    "National Day": '国庆节'
}


def get_dates():
    dates = chinese_calendar.get_dates(start_date, end_date)
    for date in dates:
        udate = date.__str__()
        uyear = date.year
        umonth = date.month
        # 一年中的第几周
        week = date.strftime('%W')
        uday = date.day
        weekday = date.isoweekday()
        is_workday = 1 if chinese_calendar.is_workday(date) else 0
        date_type = '工作日'
        if chinese_calendar.is_workday(date):
            if weekday in [6, 7]:
                date_type = '法定上班'
        else:
            is_holiday, holiday_name = chinese_calendar.get_holiday_detail(date)
            if holiday_name:
                # 节假日的中文名称
                date_type = holidays_name_dict.get(holiday_name)
            else:
                date_type = '周末'
        info = f'{date},{uyear},{umonth},{uday},{week},{weekday},{date_type},{is_workday}'
        print(info)
        yield [udate, uyear, umonth, uday, week, weekday, date_type, is_workday]


# 写入到csv文件
with open('workdays.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['date', 'uyear', 'umonth', 'uday', 'week', 'weekday', 'date_type', 'is_workday'])
    for row in get_dates():
        writer.writerow(row)
