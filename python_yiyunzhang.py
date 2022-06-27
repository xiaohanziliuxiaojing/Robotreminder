import requests
import sys
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

data_dict={}

sql = """
    SELECT b.all_num 创建组织数,case when b.credit_num IS NULL then 0 else b.credit_num end 认证组织数,c.num 首签组织数,d.origan_num 充值组织数,e.num 签署组织数,f.num 新增充值账户数,g.num 新增充值金额,
    h.num 个人注册数,i.num 发起量,l.num 签署量,m.pv 官网浏览量,m.uv 官网访问量,n.consult_uv 在线咨询量,o.origan_uv 登陆组织数,
    p.lost_origan_uv 预流失组织数  FROM
(
   SELECT "key" key FROM company LIMIT 1
)a
LEFT JOIN
(
   select count(1) num,'key' as key from user_basic_info  where valid_status=1 AND TO_CHAR(create_time,'yyyymmdd')=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd') AND  telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
    "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
)h
ON a.key=h.key
LEFT JOIN
(
    SELECT  COUNT(1) all_num,sum(IF(credit_status=1 , 1 , 0)) credit_num,'key' key FROM company
    WHERE valid_status = 1 and TO_CHAR(create_time,'yyyymmdd')=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd') AND applicant_id IN
(
select id FROM user_basic_info WHERE telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
)
)b
ON a.key=b.key
LEFT JOIN
(
    SELECT count(distinct a.com_id)num ,'key' key FROM
        (
        SELECT ROW_NUMBER() OVER(PARTITION BY com_id  ORDER BY finish_time ASC ) AS rowid,com_id,finish_time,'firstSignOrganNum' flag FROM contract_partner_task
        WHERE task_status=1 AND com_id!=0 AND user_id in (
        select id FROM user_basic_info WHERE telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016"))
        )a
        WHERE a.rowid=1  AND TO_CHAR(FROM_UNIXTIME(a.finish_time),'yyyymmdd')=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
)c
ON a.key=c.key
LEFT JOIN
(
    SELECT count(DISTINCT a.com_id) origan_num ,'key' key FROM trade a
        JOIN
        (
        SELECT a.id,b.telephone,b.name applicant_name,a.name FROM company a
        JOIN user_basic_info b
        ON a.applicant_id=b.id
        WHERE b.telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
         )b
         ON a.com_id=b.id
        WHERE a.com_id!=0 AND a.status=3 AND TO_CHAR(a.create_time,'yyyymmdd')=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')

)d
ON a.key=d.key
LEFT JOIN
(
    select count(distinct com_id)num,'key' key FROM contract_partner_task WHERE task_status=1 AND TO_CHAR(FROM_UNIXTIME(finish_time),'yyyymmdd')=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')  and user_id in (
    select id FROM user_basic_info WHERE telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
    )
)e
ON a.key=e.key
LEFT JOIN
(
select count(1)num,'key' as key From contract_master  where status=1 and com_id!=0  AND user_id in (
    select id FROM user_basic_info WHERE telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
    ) AND TO_CHAR(create_time,'yyyymmdd')=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
)i
ON a.key=i.key
LEFT JOIN
(
    SELECT count(1)num,'key' as key FROM contract_file a
    JOIN 
    (
        SELECT DISTINCT a.id,a.user_id,b.name,b.telephone FROM contract_master a
        JOIN user_basic_info b
        ON a.user_id=b.id
        WHERE  b.telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
    ) b
    ON a.master_id=b.id
    WHERE a.finish_time IS NOT NULL and TO_CHAR(FROM_UNIXTIME(finish_time),'yyyymmdd')=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
)l
ON a.key=l.key
LEFT JOIN
(
    SELECT b.num+c.num AS num,a.key FROM
    (
        SELECT 'key' key
    )a
    left JOIN
    (
    SELECT count(DISTINCT com_id) num,"key" key  FROM
      (
        SELECT min(create_time) min_time,com_id FROM trade a
        JOIN
        (
        SELECT a.id,b.telephone,b.name applicant_name,a.name FROM company a
        JOIN user_basic_info b
        ON a.applicant_id=b.id
        WHERE b.telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
        )b
        ON a.com_id=b.id
        WHERE a.com_id!=0 AND a.status=3
        group by com_id
      )
      where TO_CHAR(min_time,'yyyymmdd')=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
    )b
    on a.key=b.key
    left JOIN
    (
      SELECT count(DISTINCT user_id) num,"key" key FROM
      (
        SELECT min(a.create_time) min_time,user_id FROM trade a
        JOIN user_basic_info b
        ON a.user_id=b.id
        WHERE b.telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016") AND a.com_id=0 AND a.status=3
        group BY user_id
      )
      where TO_CHAR(min_time,'yyyymmdd')=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
    )c
    on a.key=c.key
)f
ON a.key=f.key
LEFT JOIN
(
    SELECT nvl(b.num,0)+nvl(c.num,0) num ,a.key FROM
    (
        SELECT 'key' key
    )a
    LEFT JOIN 
    (
    SELECT sum(a.amount)/100 num,'key' key FROM trade a
        JOIN
        (
        SELECT a.id,b.telephone,b.name applicant_name,a.name FROM company a
        JOIN user_basic_info b
        ON a.applicant_id=b.id
        WHERE b.telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
         )b
         ON a.com_id=b.id
        WHERE a.com_id!=0 AND a.status=3 AND TO_CHAR(create_time,'yyyymmdd')=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')

    )b
    ON a.key=b.key 
    left join
    (
        
        SELECT sum(a.amount)/100 num,'key' key FROM trade a
        JOIN
        (
        SELECT id,telephone FROM user_basic_info  b
        WHERE b.telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
         )b
         ON a.user_id=b.id
        WHERE a.com_id=0 AND a.status=3 AND TO_CHAR(create_time,'yyyymmdd')=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')

    )c
    ON a.key=c.key
)g
ON a.key=g.key
LEFT JOIN 
(
    SELECT a.pv,b.user_uv+c.com_uv uv,a.key FROM 
    (
       SELECT COUNT(1) pv,'key' as key FROM user_bury_point 
       WHERE bury_event=0 AND ds=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
       AND user_id IN (
       select id FROM user_basic_info WHERE telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
       )   
    )a
    LEFT JOIN 
    (
        SELECT COUNT(DISTINCT user_id) user_uv,'key' as key FROM user_bury_point 
        WHERE bury_event=0 AND com_id=0 AND ds=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
        AND user_id IN (
        select id FROM user_basic_info WHERE telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
        )  
    )b
    ON a.key=b.key
    LEFT JOIN 
    (
        SELECT COUNT(DISTINCT com_id) com_uv,'key' as key FROM user_bury_point 
        WHERE bury_event=0 AND com_id !=0 AND ds=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
        AND user_id IN (
        select id FROM user_basic_info WHERE telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
        )  
    )c
    ON a.key=b.key

)m
ON a.key=m.key
LEFT JOIN
(
    SELECT b.consult_user_uv+c.consult_com_uv consult_uv,a.key FROM 
    (
        SELECT 'key' as key
    )a
    LEFT JOIN 
    (
        SELECT count(DISTINCT user_id) consult_user_uv,'key' as key FROM user_bury_point 
        WHERE bury_event=1 AND com_id=0 AND ds=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
        AND user_id IN (
        select id FROM user_basic_info WHERE telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
        )
    )b
    ON a.key=b.key
    LEFT JOIN 
    (
        SELECT count(DISTINCT com_id) consult_com_uv,'key' as key FROM user_bury_point 
        WHERE bury_event=1 AND com_id !=0 AND ds=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
        AND user_id IN (
        select id FROM user_basic_info WHERE telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
        )
    )c
    ON a.key=c.key
)n
ON a.key=n.key
LEFT JOIN
(
    SELECT count(DISTINCT com_id) origan_uv,'key' as key FROM user_bury_point 
    WHERE bury_event=4 AND com_id !=0 AND ds=TO_CHAR(DATEADD(GETDATE(),-1,'dd'),'yyyymmdd')
    AND user_id IN (
    select id FROM user_basic_info WHERE telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
    )
)o
ON a.key=o.key
LEFT JOIN
(
    SELECT COUNT(DISTINCT com_id) lost_origan_uv,'key'key FROM 
    (
        SELECT com_id FROM user_bury_point 
        WHERE bury_event=4 AND com_id !=0 AND ds=TO_CHAR(DATEADD(GETDATE(),-7,'dd'),'yyyymmdd')
        AND user_id IN (
            select id FROM user_basic_info WHERE telephone NOT IN ("13691278768","15801695887","18626620909","13718560406","15713660702",
            "18369390581","13784090190","15711144248","18311384523","13398667016","18810829016")
        ) 
        GROUP BY com_id HAVING count(1)=1
    )
)p
ON a.key=p.key
    """
def getOriganData():
    '''获取组织创建数'''
    """组织创建数 create_origan_num 组织认证数 regi_origan_num"""
    """首签组织数 first_origan_num"""
    """充值组织数 pay_origan_num"""
    """签署组织数 sign_finsh_origan_num"""
    """新增充值账户（组织）数 new_pay_origan_num"""
    """新增充值金额  new_pag_num"""
    """个人注册数 person_create_num"""
    """发起量 sigin_origan_count"""
    """签署量 sigin_finsh_origan_count"""
    """官网浏览量 official_website_pv"""
    """官网访问人数 official_website_uv"""
    """在线咨询量 online_consult_uv"""
    """登录组织数 login_origan_uv"""
    """预流失组织数 lost_origan_uv"""

    data_dict = {}
    with odps.execute_sql(sql).open_reader() as reader:
            for record in reader:
                data_dict["create_origan_num"]=str(record[0])
                data_dict["regi_origan_num"]=str(record[1])
                data_dict["first_origan_num"]=str(record[2])
                data_dict["pay_origan_num"]=str(record[3])
                data_dict["sign_finsh_origan_num"]=str(record[4])
                data_dict["new_pay_origan_num"]=str(record[5])
                data_dict["new_pag_num"]=str(record[6])
                data_dict["person_create_num"]=str(record[7])
                data_dict["sigin_origan_count"]=str(record[8])
                data_dict["sigin_finsh_origan_count"]=str(record[9])
                data_dict["official_website_pv"]=str(record[10])
                data_dict["official_website_uv"]=str(record[11])
                data_dict["online_consult_uv"]=str(record[12])
                data_dict["login_origan_uv"]=str(record[13])
                data_dict["lost_origan_uv"]=str(record[14])


    return (data_dict)

def test_robot(data_dict):
    headers = {"Content-Type":"text/plain"}
    # s = "实时新增用户反馈<font color=\"warning\"> %s 人</font>，请相关同事注意." % str(num)
    s1="昨日数据详情如下：<font color=\"comment\"> </font>"
    s2="\n >组织创建数:<font color=\"warning\"> %s </font>" % (data_dict["create_origan_num"])
    s3="\n >组织认证数:<font color=\"warning\"> %s </font>" % (data_dict["regi_origan_num"])
    s4="\n >首签组织数:<font color=\"warning\"> %s </font>" % (data_dict["first_origan_num"])
    s5="\n >充值组织数:<font color=\"warning\"> %s </font>" % (data_dict["pay_origan_num"])
    s6="\n >签署组织数:<font color=\"warning\"> %s </font>" % (data_dict["sign_finsh_origan_num"])
    s7="\n >新增充值账户数(个人+组织):<font color=\"warning\"> %s </font>" % (data_dict["new_pay_origan_num"])
    s8="\n >新增充值金额(个人+组织):<font color=\"warning\"> %s </font>" % (data_dict["new_pag_num"])
    s9="\n >个人注册数:<font color=\"warning\"> %s </font>" % (data_dict["person_create_num"])
    s10="\n >发起量:<font color=\"warning\"> %s </font>" % (data_dict["sigin_origan_count"])
    s11="\n >签署量:<font color=\"warning\"> %s </font>" % (data_dict["sigin_finsh_origan_count"])
    # s12="\n >官网浏览量(非测试,个人+组织):<font color=\"warning\"> %s </font>" % (data_dict["official_website_pv"])
    # s13="\n >官网访问人数(非测试,个人+组织):<font color=\"warning\"> %s </font>" % (data_dict["official_website_uv"])
    # s14="\n >在线咨询量(非测试,个人+组织):<font color=\"warning\"> %s </font>" % (data_dict["online_consult_uv"])
    s15="\n >登录组织数(非测试):<font color=\"warning\"> %s </font>" % (data_dict["login_origan_uv"])
    s16="\n >预流失组织数(非测试):<font color=\"warning\"> %s </font>" % (data_dict["lost_origan_uv"])


    data = {
    "msgtype": "markdown",
    "markdown": {
        "content":s1+s9+s2+s3+s4+s5+s15+s6+s16+s10+s11+s7+s8
                }
       }
    # print(s1+s9+s2+s3+s4+s5+s6+s10+s11+s7+s8)
    r = requests.post(
        url='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=c389e13c-d76e-4ac7-a83f-dbfd1a22c766',
        # url='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=5b467cae-42d2-46dd-800b-dd646350fb23',
        headers=headers,
        json=data)
    print(r.text)

if __name__ == '__main__':
    data_dict=getOriganData()
    test_robot(data_dict)
