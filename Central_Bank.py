import datetime
import json
import os
import zipfile
from urllib.parse import urljoin

import pandas as pd
import requests

# 文件输出的路径
dataDir = r"c:\数据抓取\央行业务公告\data"
# chromedriver.exe 所在的路径
chromedirverpath = r"C:\Windows\System32\chromedriver.exe"

import chardet


def extract_html_from_url(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    }
    """
    从给定的URL中提取HTML内容并完成解码。

    参数:
        url (str): 目标网页的URL。

    返回:
        str: 解码后的HTML内容。
    """
    try:
        # 发送HTTP请求获取网页内容
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # 检查请求是否成功

        # 获取响应的内容并解码
        # html_content = response.content.decode("utf-8")
        detected_encoding = chardet.detect(response.content)["encoding"]
        # print(detected_encoding)
        html_content = response.content.decode(detected_encoding)
        return html_content
    except requests.RequestException as e:
        print(f"请求错误: {e}")
        raise e
        # return None
    except Exception as E:
        raise E


# def get_data1(dt, update_date, dataDir):
#
#     timestamp = int(time.time() * 1000)  # 当前毫秒级时间戳
#     headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}
#     urla = f"https://www.chinamoney.com.cn/ags/ms/cm-u-bond-publish/TicketHandle?t={timestamp}&t={timestamp}"
#     print(urla)
#     # https: // www.chinamoney.com.cn / ags / ms / cm - s - notice - query / contents?pageNo = 1 & pageSize = 15 & channelId = 2845, 2846
#
#     table = []
#     table_header = ['操作日期', '期限(天)', '交易量(Z)', '中标利率(%)', '正/逆回购', '爬取时间']
#
#     # 获取数据 1
#     response = requests.get(url=urla, headers=headers,timeout = 20)
#     sleep(5)
#     result = json.loads(response.content)
#     print(result)
#
#     # 解析数据写入列表 1
#     for item in result['data']['resultList']:
#         table_rows = []
#         table_rows.append(item['operationFromDate'])
#         table_rows.append(item['period'])
#         table_rows.append(item['dealAmount'])
#         table_rows.append(item['rate'])
#         table_rows.append(item['tradingMethod'])
#         table_rows.append(dt)
#         table.append(table_rows)
#
#     # 生成 csv 文件 1
#     df = pd.DataFrame(table, columns=table_header)
#     f_name = dataDir + f"\Central_Bank_{update_date}.csv"
#     df.to_csv(f_name, index=0, float_format='%.5f', encoding='utf-8-sig')


def get_data1(dt, update_date, dataDir, pageSize=15, get_all=False):
    """
    爬取指定日期的央行正/逆回购操作数据，并保存为CSV文件。

    参数:
    dt (str): 爬取数据的日期，格式为YYYY-MM-DD。
    update_date (str): 需要更新的数据日期，格式为YYYYMMDD。
    dataDir (str): 数据保存的目录路径。

    返回:
    无
    """
    url = "https://www.chinamoney.com.cn/ags/ms/cm-s-notice-query/contents?pageNo=1&channelId=2845,2846"
    # url = "https://www.chinamoney.com.cn/ags/ms/cm-s-notice-query/contents?pageNo=1&pageSize=15&channelId=2845,2846"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    }

    # tb_list = []
    data_from = {
        "pageNo": 1,
        "pageSize": pageSize,
    }
    res = requests.get(url, params=data_from, headers=headers)
    # res = requests.post(url, data=data_from, headers=headers)
    txt_c = json.loads(res.text)

    tb_list = []
    data_fr = pd.DataFrame()
    base_url = "https://www.chinamoney.com.cn/"
    for row in txt_c["records"]:
        url_path = row["draftPath"]
        req_url = urljoin(base_url, url_path)
        item = {"opDate": row["releaseDate"], "req_url": req_url}
        tb_list.append(item)
    # print(tb_list)
    for row in tb_list:
        if get_all or row["opDate"].strip().replace("-", "") == update_date:
            html = extract_html_from_url(row["req_url"])
            try:
                df = pd.read_html(row["req_url"])
            except Exception as e:
                print("数据异常")
                print(e)
                print(html)
                continue

            print(row["req_url"])
            print(df)
            # print(html)
            # print(len(df))
            # if len(df) == 1:
            #     df = df[0]
            # else:
            #     df = df[1]
            for dfdf in df:
                df1 = pd.DataFrame()
                for i in range(3):
                    if "期限" in dfdf.loc[0, i]:
                        df1["期限"] = [
                            x.replace("天", "") for x in [y for y in dfdf.loc[1:, i]]
                        ]
                    if "操作量" in dfdf.loc[0, i]:
                        df1["操作量"] = [
                            x.replace("亿元", "") for x in [y for y in dfdf.loc[1:, i]]
                        ]
                    if "操作利率" in dfdf.loc[0, i]:
                        df1["操作利率"] = [
                            x.replace("%", "") for x in [y for y in dfdf.loc[1:, i]]
                        ]
                df1["操作日期"] = row["opDate"]
                df1["正/逆回购"] = "逆回购" if html.find("逆回购") != -1 else "正回购"
                df1["爬取时间"] = dt
                print(df1)

                # 检测需要的字段是否存在
                if (
                    "期限" not in df1.columns
                    or "操作量" not in df1.columns
                    or "操作利率" not in df1.columns
                ):
                    print("数据异常,缺少需要的字段")
                    print(df1)
                    continue
                data_fr = pd.concat([data_fr, df1])
        else:

            pass
    # 标题格式
    table_header = ["操作日期", "期限", "操作量", "操作利率", "正/逆回购", "爬取时间"]
    f_name = os.path.join(dataDir, f"Central_Bank_{update_date}.csv")
    data_format = data_fr[table_header]
    print(data_fr[table_header])
    data_format.to_csv(
        f_name, index=0, header=table_header, float_format="%.5f", encoding="utf-8-sig"
    )


def zip_data1(dataDir, update_date):
    """
    将指定目录下的CSV和XLSX文件压缩成一个ZIP文件。

    参数:
    dataDir -- 包含要压缩的文件的目录路径。
    update_date -- 用于压缩文件名的日期字符串，格式为YYYYMMDD。

    返回:
    无返回值。函数会在指定目录下创建一个名为CENTRAL_BANK_YYYYMMDD.zip的压缩文件。
    """
    oldpath = os.getcwd()
    try:
        os.chdir(dataDir)
        paraname = "CENTRAL_BANK"

        # 新建压缩包，放文件进去,若压缩包已经存在，将覆盖。可选择用a模式，追加
        azip = zipfile.ZipFile("{0}_{1}.zip".format(paraname, update_date), "w")
        for dirpath, _dirnames, filenames in os.walk("./"):
            """dirnames,"""

            for file in filenames:
                if file.find(".csv") != -1 or file.find(".xlsx") != -1:
                    fullpath = os.path.join(dirpath, file)
                    # print(fullpath)
                    # 必须保证路径存在,将bb件夹（及其下aa.txt）添加到压缩包,压缩算法LZMA
                    # azip.write(fullpath, compress_type=zipfile.ZIP_LZMA)
                    azip.write(fullpath)
                    # os.remove(file)  # 删除文件
        # 关闭资源
        azip.close()
        os.chdir(oldpath)
    except Exception as E:
        print(E)
        os.chdir(oldpath)


if __name__ == "__main__":

    dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_date = datetime.datetime.now().strftime("%Y%m%d")

    # 路径不存在时，自动创建
    if os.path.exists(dataDir) is False:
        os.makedirs(dataDir)

    # 清空目录内所有文件
    filelists = os.listdir(dataDir)
    os.chdir(dataDir)
    for vfile in filelists:
        os.remove(vfile)

    # 获取数据
    try:
        get_data1(dt, update_date, dataDir)
        print("get_data1 successful.")
    except Exception as E:
        print(E)
        print("下载数据异常，请稍后重试。")
    finally:
        # 生成压缩包
        zip_data1(dataDir, update_date)
        print("zip data1 successful.")
