import pymysql
import requests
import re
from bs4 import BeautifulSoup
import time, datetime

origin_url = 'http://www.moa.gov.cn'
catid = 51


def conn():  # 连接数据库
    db = pymysql.connect(host='47.99.78.253', user='hn_xiangtu_site', password='henanxiangtu2099', db='hn_xiangtu_site',
                         port=3306)
    print('已连接数据库')
    return db


header = {
    'Accept-Language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'
}


def get_html(url):  # 爬取主页
    try:
        html = requests.get(url, headers=header)  # 使用requests库爬取
        html.encoding = html.apparent_encoding
        if html.status_code == 200:  # 如果状态码是200，则表示爬取成功
            print(url + '获取成功')
            # print(html.text)
            return html.text  # 返回H5代码
        else:  # 否则返回空
            print('获取失败')
            return None
    except:  # 发生异常返回空
        print('获取失败')
        return None


def get_url(html):  # 解析首页得到所有的网页和课程id
    class_list = []  # 定义存放class_id的列表
    class_info = BeautifulSoup(html, "html.parser")
    class_div = class_info.find('div', {'class': 'pub-media1-txt-list'})  # 找到存放class_id的div
    class_li = class_div.find('ul').find_all('li')  # 找到div下的ul标签内的所有li
    for class_id in class_li:
        href = class_id.a.get('href').replace('./', '/')
        class_list.append(origin_url + '/xw/qg' + href)  #
    return class_list


# 过滤HTML中的标签
# 将HTML中标签等信息去掉
# @param htmlstr HTML字符串.
def filter_tags(htmlstr):
    # 先过滤CDATA
    re_cdata = re.compile('//<!\[CDATA\[[^>]*//\]\]>', re.I)  # 匹配CDATA
    re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)  # Script
    re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)  # style
    re_br = re.compile('<br\s*?/?>')  # 处理换行
    # re_h = re.compile('</?\w+[^>]*>')  # HTML标签
    re_comment = re.compile('<!--[^>]*-->')  # HTML注释
    s = re_cdata.sub('', htmlstr)  # 去掉CDATA
    s = re_script.sub('', s)  # 去掉SCRIPT
    s = re_style.sub('', s)  # 去掉style
    s = re_br.sub('\n', s)  # 将br转换为换行
    # s = re_h.sub('', s)  # 去掉HTML 标签
    s = re_comment.sub('', s)  # 去掉HTML注释
    # 去掉多余的空行
    blank_line = re.compile('\n+')
    s = blank_line.sub('\n', s)
    return s


def get_info(word_html, type_name=''):  # 爬取所有的单词、发音、翻译

    htmlContent = BeautifulSoup(word_html, "html.parser")
    # print(htmlContent.title)
    title = htmlContent.find('h1', {'class': 'bjjMTitle'}).get_text()  # 标题
    dclist = htmlContent.find('div', {'class': 'bjjMAuthorBox'}).find_all('span', {'class': 'dc_3'})
    print('dclist', dclist)
    time = dclist[0].get_text()
    author = dclist[1].get_text()
    source = dclist[2].get_text()
    # for key,val in dclist:
    #     if key == 0:
    #
    #     if key == 2:
    #         source = val.get_text()
    # time = htmlContent.findall('span', {'class': 'dc_3'}).get_text()  # 日期
    # source = htmlContent.findall('span', {'class': 'dc_3'}).get_text()  # 日期
    rich = htmlContent.find('div', {'class': 'TRS_Editor'}).decode_contents()  # 日期

    article = filter_tags(rich)

    content = article.replace("\n", "")

    # #content = content.rtrip()
    # content = content.replace("\n", "")

    print(title, time, source, content)
    print('创建数据成功')
    return {'title': title, 'time': time, 'source': source, 'content': content, 'author': author}


def insert_news(info_data, db):  # 爬取数据到数据库
    cursor = db.cursor()  # 创建一个游标
    print(info_data)
    # word_dict是一个字典，模型：{'distant': ['[ˈdistənt]', 'adj. 远的；遥远的；疏远的；不亲近的', '考研必备词汇']}
    sql = 'INSERT INTO v9_news(catid, title, thumb, inputtime,updatetime,status,username,sysadd) values(%s, %s, %s, %s, %s, %s, %s, %s)'  # 构造sql语句
    nid = ''
    try:
        # title,time,source,content
        timeArray = time.strptime(info_data['time'] + ':00', "%Y-%m-%d %H:%M:%S")
        timeStamp = int(time.mktime(timeArray))
        print(timeStamp)
        cursor.execute(sql, (catid, info_data['title'], '', timeStamp, timeStamp, '99', 'admin', '1'))
        nid = db.insert_id()
        print(db.insert_id())
        db.commit()  # 插入数据
        print('数据插入成功')
        db.close()  # 关闭数据库
        print('数据库成功关闭')
    except  Exception as e:
        print('写入数据库失败', e)
        # db.rollback()  # 回滚
    return nid


def insert_news_data(info_data, id, db):  # 爬取数据到数据库
    cursor = db.cursor()  # 创建一个游标
    sql = 'INSERT INTO v9_news_data(id, content,copyfrom,author) values(%s, %s,%s,%s)'  # 构造sql语句
    try:
        # title,time,source,content
        conpyfrom = info_data['source']
        if conpyfrom == '':
            conpyfrom = '农业农村部'
        # content = "<div class='py_rich_text'>"+info_data['content']+"</div>"
        content = info_data['content']
        cursor.execute(sql, (id, content, conpyfrom, info_data['author']))
        db.commit()  # 插入数据
        print('文章详情插入成功')
        db.close()  # 关闭数据库
    except  Exception as e:
        print('写入文章详情失败', e)
        # db.rollback()  # 回滚
    print('数据库成功关闭')


def main():
    db = conn()
    base_url = origin_url + '/xw/qg/index_page.htm'  # 网址
    for pageIdx in range(0, 45):
        print(pageIdx, base_url)
        if pageIdx == 0:
            base_url2 = base_url.replace('/index_page.htm', '/')
        else:
            base_url2 = base_url.replace('page', str(pageIdx))
        print('base_url2', base_url2)

        base_html = get_html(base_url2)  # 得到列表页
        urls = get_url(base_html)  # 得到所有class_id值
        print(urls)
        # print(class_id)
        print('爬取主页')

        for url in urls:  # word_all为class_id所有可能的取值
            # 拼接URL
            course_url = url
            print('course_url', course_url.index('www.moa.gov.cn'))
            try:
                if course_url.index('www.moa.gov.cn') >= 0:
                    print('开始爬取' + course_url)
                    info_html = get_html(course_url)
                    print('开始爬取数据')
                    info_dict = get_info(info_html)  # 得到数据字典
                    # print(info_dict)
                    # exit()
                    print('开始存储数据')
                    db = conn()
                    nid = insert_news(info_dict, db)  # 存储数据
                    print(nid)
                    db2 = conn()
                    insert_news_data(info_dict, nid, db2)
            except Exception as e:
                print('网址无效', e)


if __name__ == '__main__':
    main()
