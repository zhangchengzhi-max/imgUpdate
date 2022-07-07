import time
import pymysql
import requests
import re
import logging


#日志输入信息-->时间，py文件名日志级别，信息,第几行
logging.basicConfig(level=logging.INFO,
                    filename='log/updatelogroom.txt',
                    filemode='a',
                    format='%(asctime)s - %(filename)s - %(message)s - %(levelname)s ')



db = pymysql.connect(host='10.234.128.66', user='live_app', password='XEaEyOBZE', port=3332, db='live', charset='utf8')
# db = pymysql.connect(host='10.234.128.66', user='live_app', password='@x{adSSWy', port=3339, db='live', charset='utf8')

def urlCheck(url):
    if "vodctz2y190.ws.126.net" in url:
        url_check = url.replace("vodctz2y190.ws.126.net", 'live163.nos2-i.service.163.org/vodctz2y190')
        return requests.head(url_check).status_code == 200
    else:
        return False

def urlReplace(url):
    url_update = url.replace('vodctz2y190.ws.126.net', 'live163.ws.126.net/vodctz2y190')
    return url_update

pageSize = 10
pattern = '(http://vodctz2y190\.ws\.126\.net/.+?\.mp4)'

def updateRecord():
    offset = 0
    while True:
        sql1 = f"SELECT id,mp4_url FROM video_record  order by start_time desc limit {pageSize} offset {offset}"
        # sql1 = f"SELECT id, mp4_url FROM video_record ORDER BY id LIMIT {pageSize} OFFSET {offset}"
        cursor = db.cursor()
        cursor.execute(sql1)
        result = cursor.fetchall()
        # print(result)
        for r in result:
            id = r[0]
            if r[1] is None or r[1] == "" :
                continue
            mp4Url = r[1]
            print("record",urlCheck(mp4Url),mp4Url)
            if urlCheck(mp4Url):
                newUrl = urlReplace(mp4Url)
                sql2 = f"update video_record set mp4_url='{newUrl}' where id = {id}"
                # print(sql2)
                cursor2 = db.cursor()
                cursor2.execute(sql2)
                db.commit()

                #写入更新数据的日志
                logging.info(f"table:room，id:{id}，{mp4Url} --> {newUrl}")
            else:
                # logging.error(f"url check failed:{mp4Url}")
                logging.error(f"id:{id}，  url check failed:{mp4Url}")
        # print(len(result))
        offset += len(result)
        time.sleep(2)
        # 判断最后一页
        if len(result) < pageSize:
            # print(str(len(result)) + "条数据")
            print("最后一页有{}条数据".format(len(result)))
            break


def updateRoom():
    offset = 0
    while True:
        sql1 = f"SELECT id, config FROM room ORDER BY start_date desc LIMIT {pageSize} OFFSET {offset}"
        cursor = db.cursor()
        cursor.execute(sql1)
        result = cursor.fetchall()

        for r in result:
            id = r[0]
            if r[1] is None or r[1] == "":
                continue

            config = r[1]

            url_list = re.findall(pattern, config)
            for url in url_list:
                print("room",urlCheck(url),url)
                if urlCheck(url):
                    newUrl = urlReplace(url)
                    config = config.replace(url, newUrl)
                    logging.info(f"table:room，id:{id}，{url} --> {newUrl}")
                else:
                    # logging.error(f"url check failed:{url}")
                    logging.error(f"id:{id}，  url check failed:{url}")
            if config != r[1]:
                sql2 = f"update room set config='{config}' where id = {id}"
                # print(sql2)
                cursor2 = db.cursor()
                cursor2.execute(sql2)
                db.commit()

        # print(len(result))
        time.sleep(1)
        offset += len(result)

        # 判断最后一页
        if len(result) < pageSize:
            # print(str(len(result)) + "条数据")
            print("最后一页有{}条数据".format(len(result)))
            break


# def update(pageSize,offset,table_name,column,x):
#     global result
#     while True:

#         try:
#             # 首先进行分页查询
#             sql1 = "SELECT " + table_name + ".`id`," + table_name + ".`" + column + "` FROM " + table_name + " ORDER BY " + table_name + ".`id` LIMIT " + str(
#                 pageSize) + " OFFSET " + str(offset)
#             cursor = db.cursor()
#             cursor.execute(sql1)
#             result = cursor.fetchall()
#         except Exception as ex:
#             logging.error(ex)

#         try:
#             for r in result:

#                 old_column = r[1]
#                 new_column = ""

#                 if x != 1:

#                     # 不需要正则
#                     if "vodctz2y190.ws.126.net" in str(r[1]):
#                         # 进行域名替换的工作
#                         url_update = imgCheck(str(r[1]))
#                         new_column = url_update

#                 if x == 1:

#                     # 需要正则匹配
#                     pattern = '(http://vodctz2y190\.ws\.126\.net/.+?\.mp4)'
#                     url_list = re.findall(pattern, str(r[1]))
#                     for url in url_list:
#                         # 进行域名替换工作
#                         url_update = imgCheck(url)
#                         # column中的url替换为url_update
#                         new_column = old_column.replace(url, url_update)
#                         old_column = new_column


#                 if new_column != "":
#                     sql2 = "UPDATE " + table_name + " SET " + table_name + ".`" + column + "`  = '" + new_column + "'   WHERE " + table_name + ".`id` = " + str(r[0])
#                     cursor2 = db.cursor()
#                     cursor2.execute(sql2)
#                     db.commit()


#                     #写入更新数据的日志
#                     logging.info("id={}的数据，原数据为{}，更新后为{}".format(r[0],r[1],new_column))
#         except Exception as ex:
#             logging.error(ex)


#         # 这一页处理完，开始下一页
#         offset += pageSize
#         print("第{}页".format(offset / pageSize))
#         logging.info("第{}页".format(offset / pageSize))

#         # 判断最后一页
#         if len(result) < pageSize:
#             # print(str(len(result)) + "条数据")
#             print("最后一页有{}条数据".format(len(result)))
#             break


if __name__ == '__main__':
    # updateRecord()
    updateRoom()

    # pageSize = 2
    # offset = 0

    # #update(pageSize,offset,表名，列名，x)----------标识符x=1,说明字段中有多个url，需要正则匹配，否则不需要
    # update(pageSize,offset,"room","config",1)
    # update(pageSize,offset,"video_record","mp4_url",0)

