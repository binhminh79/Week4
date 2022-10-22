import pandas as pd
from functools import reduce

employees_csv = "data/employees.csv"
salaries_csv = "data/salaries.csv"

#dùng function map để tạo ra 1 list chứa danh sách là các full name của employees
def question_3():
    df = pd.read_csv(employees_csv)
    fullname = map(lambda x, y: f"{x} {y}", df['first_name'], df['last_name'])
    return list(fullname)

#dùng function filter và reduce để tính tổng lương của các nhân viên hiện tại.
def question_4():
    df = pd.read_csv(salaries_csv)
    return reduce(lambda x, y : x+y, df['salary'])

print(question_4())