#!/usr/bin/python
# -*- coding: UTF-8 -*-
# @Author  : ren@uwa4d.com

import pymysql

g_db_conn = pymysql.connect(host='localhost', user='root', passwd='', port=3306, charset='utf8')
g_db_cur = g_db_conn.cursor()  # 获取一个游标对象


def create_db(db_name):
    g_db_cur.execute("CREATE DATABASE %s" % db_name)


def use_db(db_name):
    g_db_cur.execute("USE %s" % db_name)


def drop_table(table_name):
    try:
        sql="DROP TABLE %s"
        g_db_cur.execute(sql % (table_name))
        return True
    except Exception, e:
        print e
        return False
    return False


def test_table_when_create(table_name):
    row = g_db_cur.execute("SELECT * FROM %s" % table_name)
    if row >= 1:
        data = g_db_cur.fetchall()
        print data
        g_db_cur.execute("DELETE FROM %s WHERE 1" % table_name)
        return True


def create_tbl_question(): #问题表 #避免循环外键，将answer提出单独列表
    try:
        table_name='tbl_question'
        g_db_cur.execute("CREATE TABLE %s "
                         "(question_id VARCHAR(200) NOT NULL PRIMARY KEY, "
                         "link VARCHAR(512), "
                         "state VARCHAR(100), "
                         "title VARCHAR(200), "
                         "update_time VARCHAR(200),"
                         "reply_num INT(10),"
                         "following_num INT(10))"
                         "CHARSET=UTF8" %table_name)
        g_db_cur.execute("INSERT INTO tbl_question VALUES ('00000000001', 'answers.unity3d.com/aa',  'solved', 'answers_title','update_time')")
        test_table_when_create(table_name)
    except Exception, e:
        print e
        return False
    return False


def create_tbl_content(): #内容表，答案也属于内容，问题-答案联系放在另一表中
    try:
        table_name = 'tbl_content'
        g_db_cur.execute("CREATE TABLE %s "
                         "(content_id VARCHAR(200) NOT NULL PRIMARY KEY UNIQUE, "
                         "content VARCHAR(5000),"
                         "question_id VARCHAR(200), "
                         "vote INT(10),"

                         "FOREIGN KEY (question_id) REFERENCES tbl_question(question_id)) "
                         "CHARSET=UTF8" %table_name)
        g_db_cur.execute("INSERT INTO tbl_content VALUES ('00000000001', 'content',NULL, 11)")
        test_table_when_create(table_name)
    except Exception, e:
        print e
        return False
    return False


def create_tbl_tag():
    try:
        table_name = 'tbl_tag'
        g_db_cur.execute("CREATE TABLE %s "
                         "(tag_id VARCHAR(200) NOT NULL PRIMARY KEY UNIQUE, "
                         "tag VARCHAR(5000)) "
                         "CHARSET=UTF8" %table_name)
        g_db_cur.execute("INSERT INTO %s VALUES ('00000000001', 'tag')" % table_name)
        test_table_when_create(table_name)
    except Exception, e:
        print e
        return False
    return False

def create_tbl_comment():
    try:
        table_name = 'tbl_comment'
        g_db_cur.execute("CREATE TABLE %s "
                         "(comment_id VARCHAR(200) NOT NULL PRIMARY KEY UNIQUE, "
                         "comment VARCHAR(5000), "
                         "content_id VARCHAR(200),"
                         "FOREIGN KEY (content_id) REFERENCES tbl_content(content_id))"
                         "CHARSET=UTF8" %table_name)
        #g_db_cur.execute("INSERT INTO %s VALUES ('00000000001', 'tag')" % table_name)
        test_table_when_create(table_name)
    except Exception, e:
        print e
        return False
    return False

def create_tbl_question_tag():
    try:
        table_name = 'tbl_question_tag'
        g_db_cur.execute("CREATE TABLE %s "
                         "(question_id VARCHAR(200), "
                         "tag_id VARCHAR(200) ,"
                         "PRIMARY KEY (question_id, tag_id),"
                         "FOREIGN KEY(question_id) REFERENCES tbl_question(question_id),"
                         "FOREIGN KEY(tag_id) REFERENCES tbl_tag(tag_id))"
                         "CHARSET=UTF8" %table_name)
      #  g_db_cur.execute("INSERT INTO %s VALUES ()" % table_name)
        test_table_when_create(table_name)
    except Exception, e:
        print e
        return False
    return False

def create_tbl_question_answer():
    try:
        table_name = 'tbl_question_answer'
        g_db_cur.execute("CREATE TABLE %s "
                         "(question_id VARCHAR(200), "
                         "content_id VARCHAR(200),"
                         "PRIMARY KEY (question_id, content_id),"
                         " FOREIGN KEY(question_id) REFERENCES tbl_question(question_id),"
                         " FOREIGN KEY(content_id) REFERENCES tbl_content(content_id))"
                         "CHARSET=UTF8" %table_name)
      #  g_db_cur.execute("INSERT INTO %s VALUES ()" % table_name)
        test_table_when_create(table_name)
    except Exception, e:
        print e
        return False
    return False

def create_tbl_question_qstcontent():
    try:
        table_name = 'tbl_question_qstcontent'
        g_db_cur.execute("CREATE TABLE %s "
                         "(question_id VARCHAR(200), "
                         "qstcontent_id VARCHAR(200) ,"
                         "PRIMARY KEY (question_id, qstcontent_id),"
                         "FOREIGN KEY(question_id) REFERENCES tbl_question(question_id),"
                         "FOREIGN KEY(qstcontent_id) REFERENCES tbl_content(content_id))"
                         "CHARSET=UTF8" %table_name)
      #  g_db_cur.execute("INSERT INTO %s VALUES ()" % table_name)
        test_table_when_create(table_name)
    except Exception, e:
        print e
        return False
    return False




def create_table():

    try:
        g_db_cur.execute("CREATE TABLE table_projects (p_id VARCHAR(100), p_user VARCHAR(100), p_name VARCHAR(100),p_apkname VARCHAR(100),p_teststate VARCHAR(100), p_device VARCHAR(100), p_time_pushtodevice BIGINT, p_time_lastupdate BIGINT, p_time_analysis_complete BIGINT) CHARSET=UTF8")
        g_db_cur.execute("INSERT INTO table_projects VALUES (0111111, 'test_user', 'test_pakage', 'test_apk', 'db_testing','db_device',111, 1111111,0 )")
        row=g_db_cur.execute("SELECT * FROM table_projects")
        if row>=1:
            data = g_db_cur.fetchall()
            print data
            g_db_cur.execute("DELETE FROM table_projects WHERE p_id=0111111")
            return True
    except Exception, e:
        print e
        return False
    return False



if __name__ == '__main__':
    #create_db("uwa_spider")
    use_db("uwa_spider")
    #drop_table("table_projects")

    #create_tbl_question()
   # create_tbl_content()
   # create_tbl_tag()
   # create_tbl_comment()
  #  create_tbl_question_tag()
  #  create_tbl_question_answer()
    create_tbl_question_qstcontent()
#    drop_table("tbl_comment")
#    drop_table("tbl_question_tag")
 #   drop_table("tbl_question_answer")
#    drop_table("tbl_content")
#    drop_table("tbl_tag")
 #   drop_table("tbl_question")
    g_db_conn.commit()

