import requests
import re
import threading
user=input('请输入要搜索的微信公众号或微信号:')
headers={'user-agent':'Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)'}
url='http://weixin.sogou.com/weixin?type=1&s_from=input&query={}&ie=utf8&_sug_=y&_sug_type_=&w=01015002&oq=jike&ri=0&sourceid=sugg&stj=0%3B0%3B0%3B0&stj2=0&stj0=0&stj1=0&hp=36&hp1=&sut=4432&sst0=1529305369937&lkt=5%2C1529305367635%2C1529305369835'.format(user.rstrip())


def zhuaqu():
    r = requests.get(url=url, headers=headers)
    rsw = re.findall('src=.*&timestamp=.*&ver=.*&signature=.*', str(r.text))
    print(r.text)
    if '验证码' in str(r.text):
        print('[-]发现验证码请访问URL:{}后在重新运行此脚本'.format(r.url))
        exit()
    else:
        cis = re.findall('.*?==', str(rsw[0]))
        qd = "".join(cis)
        qd2 = "{}".format(qd)
        qd3 = qd2.replace(';', '&')
        urls = 'https://mp.weixin.qq.com/profile?'.strip() + qd3
        uewq=requests.get(url=urls,headers=headers)
        if '验证码' in str(uewq.text):
                print('[-]发现验证码请访问URL:{}后在重新运行此脚本'.format(uewq.url))
                exit()
        else:
                ldw = re.findall('src = ".*?" ; ', uewq.text)
                ldw2=re.findall('timestamp = ".*?" ; ',uewq.text)
                ldw3=re.findall('ver = ".*?" ; ',uewq.text)
                ldw4=re.findall('signature = ".*?"',uewq.text)
                ldws="".join(ldw)
                ldw2s="".join(ldw2)
                ldw3s="".join(ldw3)
                ldw4s="".join(ldw4)
                ldwsjihe=ldws+ldw2s+ldw3s+ldw4s
                fk=ldwsjihe.split()
                fkchuli="".join(fk)
                gs=fkchuli.replace('"','')
                hew=gs.replace(';','&')
                wanc="http://mp.weixin.qq.com/profile?"+hew
                xiau=requests.get(url=wanc,headers=headers)
                houxu=re.findall('{.*?}',xiau.content.decode('utf-8'))
                title=re.findall('"title":".*?"',str(houxu))
                purl=re.findall('"content_url":".*?"',str(houxu))
                for i in range(0,len(title)):
                    jc='{}:{}'.format(title[i],'https://mp.weixin.qq.com'+purl[i]).replace('"','')
                    jc2=jc.replace('content_url','')
                    jc3=jc2.replace(';','&')
                    print(jc3)

t=threading.Thread(target=zhuaqu,args=())
t.start()