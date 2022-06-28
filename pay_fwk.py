# -*- coding:utf-8 -*-
import pymysql
import time
import pdb
import requests


# db='dev_room',
# user = 'rd',
# passwd = 'FCJJB648V8Y3wFKE',
# host= 'test-pub-mysql001.myrrx.com',
# use_unicode=1, 
# charset='utf8'




#连接线上数据库并获取数据
def getdata(sql):
    #连接目标数据库
    conn = pymysql.connect(host='prod.rrx.mysql02.srv',
                          user='tongji',
                          port=3306,
                          passwd='FCJJB648V8Y3wFKE',
                          db='jjd_card',
                          use_unicode=1,
                          charset='utf8'
                        )
    #使用cursor()方法获取操作游标
    cur = conn.cursor()
    #编写sql语句
    try:
        cur.execute(sql)  #执行sql语句
        result = cur.fetchone()[0]
        return str(result)
    except Exception as e:
           raise e
    finally:
        conn.commit()    #递交
        cur.close()     #关闭游标
        conn.close()    #数据库关闭




#创建服务卡机器人
def fwk_robot(number):
    headers = {"Content-Type":"text/plain"}
    s1="服务卡昨日数据详情如下(已去白名单)：<font color=\"comment\"> </font>"
    s2="\n >访问人数:<font color=\"warning\"> %s </font>" % (data_dict["fwk_dl_r_rst"])
    s3="\n >注册人数:<font color=\"warning\"> %s </font>" % (data_dict["fwk_zc_r_rst"])
    s4="\n >开通服务卡人数:<font color=\"warning\"> %s </font>" % (data_dict["fwk_kt_r_rst"])
    s5="\n >创建服务的人数:<font color=\"warning\"> %s </font>" % (data_dict["fw_cz_r_rst"])
    s51="\n >发布服务的条数:<font color=\"warning\"> %s </font>" % (data_dict["fw_fb_r_rst"])
    s6="\n >分享查看人数:<font color=\"warning\"> %s </font>" % (data_dict["fwk_fx_r_rst"])
    s7="\n >浏览服务卡人数:<font color=\"warning\"> %s </font>" % (data_dict["fwk_lla_r_rst"])
    s8="\n >浏览服务人数:<font color=\"warning\"> %s </font>" % (data_dict["fw_lla_r_rst"])
    s9="\n >关注服务卡人数:<font color=\"warning\"> %s </font>" % (data_dict["fwk_gz_r_rst"])
    s10="\n >收藏服务卡人数:<font color=\"warning\"> %s </font>" % (data_dict["fwk_sc_r_rst"])
    s11="\n >咨询人数:<font color=\"warning\"> %s </font>" % (data_dict["fwk_zx_r_rst"])
    s12="\n >下单人数:<font color=\"warning\"> %s </font>" % (data_dict["order_count_r_rst"])
    s13="\n >下单次数:<font color=\"warning\"> %s </font>" % (data_dict["order_count_rst"])
    s14="\n >交易金额（支付成功）:<font color=\"warning\"> %s </font>" % (data_dict["order_amount_rst"])
    s15="\n >交易人数（支付成功）:<font color=\"warning\"> %s </font>" % (data_dict["order_r_rst"])
    s16="\n >平台收入:<font color=\"warning\"> %s </font>" % (data_dict["shouru_rst"])
    data = {
    "msgtype": "markdown",
    "markdown": {
        "content":s1+s2+s3+s4+s5+s51+s6+s7+s8+s9+s10+s11+s12+s13+s14+s15+s16
                }
       }

    r = requests.post(
        url='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=8268a85e-aaf5-45d6-82c0-4bf863eba267',
        headers=headers,
        json=data)
    print(r.text)



if __name__ == "__main__":

    #访问人数
    fwk_dl_r = "SELECT count(DISTINCT uid) from user_access WHERE DATEDIFF(create_time,NOW())=-1 and uid not in (34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    fwk_dl_r_rst = getdata(fwk_dl_r)

    #注册人数
    fwk_zc_r = " SELECT count(uid) from user_account WHERE DATEDIFF(create_time,NOW())=-1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    fwk_zc_r_rst = getdata(fwk_zc_r)

    #开通服务卡人数
    fwk_kt_r = "SELECT COUNT(DISTINCT uid) from service_card WHERE DATEDIFF(create_time,NOW())= -1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    fwk_kt_r_rst = getdata(fwk_kt_r)

    #创建服务的人数
    fw_cz_r = "SELECT COUNT(DISTINCT uid) from service WHERE  DATEDIFF(create_time,NOW())= -1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    fw_cz_r_rst = getdata(fw_cz_r)

    #发布服务的条数
    fw_fb_r = "SELECT COUNT(1) from service WHERE  DATEDIFF(create_time,NOW())= -1 and status=1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    fw_fb_r_rst = getdata(fw_fb_r)

    #分享查看人数
    fwk_fx_r = "SELECT COUNT(DISTINCT uid) from user_access WHERE type = 0 and DATEDIFF(create_time,NOW())= -1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    fwk_fx_r_rst = getdata(fwk_fx_r)

    #咨询人数
    fwk_zx_r = "SELECT COUNT(DISTINCT uid) from user_access WHERE type = 1 and DATEDIFF(create_time,NOW())= -1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    fwk_zx_r_rst = getdata(fwk_zx_r)

    #浏览服务卡人数
    fwk_lla_r = " SELECT COUNT(DISTINCT uid) from user_follow WHERE source_type = 0 and type = 2 and DATEDIFF(create_time,NOW())=-1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    fwk_lla_r_rst = getdata(fwk_lla_r)

    #浏览服务人数
    fw_lla_r = " SELECT COUNT(DISTINCT uid) from user_follow WHERE source_type = 1 and type = 2 and DATEDIFF(create_time,NOW())=-1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134) "
    fw_lla_r_rst = getdata(fw_lla_r)

    #授权手机号人数
    fw_tel_r = " SELECT COUNT(DISTINCT uid) from user_credit_info WHERE credit_status = 1 and DATEDIFF(create_time,NOW())=-1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    fw_tel_r_rst = getdata(fw_tel_r)

    #关注服务卡人数
    fwk_gz_r = " SELECT COUNT(DISTINCT uid) from user_follow WHERE source_type = 0 and type = 1 and DATEDIFF(create_time,NOW())= -1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    fwk_gz_r_rst = getdata(fwk_gz_r)

    #收藏服务卡人数
    fwk_sc_r = " SELECT COUNT(DISTINCT uid) from user_follow WHERE source_type = 0 and type = 0 and DATEDIFF(create_time,NOW())= -1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    fwk_sc_r_rst = getdata(fwk_sc_r)

    #下单人数
    order_count_r = "SELECT COUNT(DISTINCT uid) from agreement WHERE status in (1,2) and DATEDIFF(create_time,NOW())=-1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    order_count_r_rst = getdata(order_count_r)

    #下单次数
    order_count = "SELECT COUNT(original_id) from agreement WHERE status in (1,2) and DATEDIFF(create_time,NOW())=-1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    order_count_rst = getdata(order_count)

    #交易金额
    order_amount = "SELECT ifnull(round(sum(amount/100),2),0) from trade WHERE trade_status = 3 and DATEDIFF(create_time,NOW())= -1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    order_amount_rst = getdata(order_amount)

    #交易人数
    order_r = "SELECT count(DISTINCT uid) from trade WHERE trade_status = 3  and DATEDIFF(create_time,NOW())= -1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    order_r_rst = getdata(order_r)

    #平台收入
    shouru = "SELECT ifnull(round(sum(amount/100),2),0) from user_account_list WHERE uid = 1 and valid_status = 1  and DATEDIFF(create_time,NOW())= -1 and uid not in (621680,34552,1108694,2753745,3064101,3982535,4000738,4000891,4001073,2684201,3432314,2615123,2686134)"
    shouru_rst = getdata(shouru)


    def getOriganData(fwk_dl_r_rst,fwk_zc_r_rst,fwk_kt_r_rst,fw_cz_r_rst,fw_fb_r_rst,fwk_fx_r_rst,fwk_lla_r_rst,fw_lla_r_rst,fw_tel_r_rst,fwk_gz_r_rst,fwk_sc_r_rst,fwk_zx_r_rst,order_count_r_rst,order_count_rst,order_amount_rst,order_r_rst,shouru_rst):
        data_dict = {}
        data_dict["fwk_dl_r_rst"]= fwk_dl_r_rst
        data_dict["fwk_zc_r_rst"]= fwk_zc_r_rst
        data_dict["fwk_kt_r_rst"]= fwk_kt_r_rst
        data_dict["fw_cz_r_rst"]= fw_cz_r_rst,
        data_dict["fw_fb_r_rst"]= fw_fb_r_rst,
        data_dict["fwk_fx_r_rst"]= fwk_fx_r_rst
        data_dict["fwk_lla_r_rst"]= fwk_lla_r_rst
        data_dict["fw_lla_r_rst"]= fw_lla_r_rst
        data_dict["fw_tel_r_rst"]= fw_tel_r_rst
        data_dict["fwk_gz_r_rst"]= fwk_gz_r_rst
        data_dict["fwk_sc_r_rst"]= fwk_sc_r_rst
        data_dict["fwk_zx_r_rst"]= fwk_zx_r_rst
        data_dict["order_count_r_rst"]= order_count_r_rst
        data_dict["order_count_rst"]= order_count_rst
        data_dict["order_amount_rst"]= order_amount_rst
        data_dict["order_r_rst"]= order_r_rst
        data_dict["shouru_rst"]= shouru_rst
        return (data_dict)

    data_dict =  getOriganData(fwk_dl_r_rst,fwk_zc_r_rst,fwk_kt_r_rst,fw_cz_r_rst,fw_fb_r_rst,fwk_fx_r_rst,fwk_lla_r_rst,fw_lla_r_rst,fw_tel_r_rst,fwk_gz_r_rst,fwk_sc_r_rst,fwk_zx_r_rst,order_count_r_rst,order_count_rst,order_amount_rst,order_r_rst,shouru_rst)
    fwk_robot(data_dict)

