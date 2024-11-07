import datetime
import os
import Central_Bank


def test_bank():
    """
    测试央行业务公告数据获取功能。

    本函数尝试调用Central_Bank模块的get_data1方法来获取更新的数据。
    如果成功，将打印成功消息；如果失败，则打印错误信息。
    """
    dataDir = r"d:\数据抓取\央行业务公告\data"
    os.makedirs(dataDir, exist_ok=True)
    dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_date = datetime.datetime.now().strftime("%Y%m%d")
    try:
        Central_Bank.get_data1(dt, update_date, dataDir, pageSize=70, get_all=True)
        print("get_data1 successful.")
    except Exception as E:
        print(E)
        print("下载数据异常，请稍后重试。")
    finally:
        # 生成压缩包
        Central_Bank.zip_data1(dataDir, update_date)
        print("zip data1 successful.")
