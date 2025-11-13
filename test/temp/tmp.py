import pandas as pd

# 1. 初始化包含10个学生信息的DataFrame
data = {
    '学生姓名': ['张三', '李四', '王五', '赵六', '钱七', '孙八', '周九', '吴十', '郑一', '王二'],
    '年龄': [20, 17, 19, 18, 21, 16, 22, 19, 18, 20]
}
df = pd.DataFrame(data)

print("初始学生信息：")
print(df)
print("\n" + "-"*50 + "\n")

# 2. 筛选出年龄大于18岁的学生姓名，存入列表
adult_names = [p.学生姓名 for p in df.itertuples() if p.年龄 > 18]
print("年龄大于18岁的学生姓名：")
print(adult_names)
print("\n" + "-"*50 + "\n")


print("修改年龄前打印年龄大于18岁的学生姓名：")
print(adult_names)

# 新增：修改张三的年龄为16岁
df.loc[df['学生姓名'] == '张三', '年龄'] = 16
print("修改张三年龄后的学生信息：")
print(df)
print("\n" + "-"*50 + "\n")

print(f"修改年龄后再次打印年龄大于18岁的学生姓名：{adult_names}")
print()
# 3. 从列表中删除某个学生的名字（这里以删除'张三'为例）

# if '张三' in adult_names:
#     adult_names.remove('张三')
#     print("删除'张三'后的列表：")
#     print(adult_names)
# else:
#     print("要删除的学生不在列表中")


def divide(a, b):
    try:
        return a / b
    except ZeroDivisionError as e:
        # Log the error locally, then re-throw
        print(f"Error in divide(): {e}. Re-throwing to caller.")
        raise  # Re-throw the ZeroDivisionError

# Caller function
def main():
    try:
        result = divide(10, 0)
        print(f"Result: {result}")
    except ZeroDivisionError as e:
        # Caller handles the re-thrown exception
        print(f"Caller caught: {e}")

main()

