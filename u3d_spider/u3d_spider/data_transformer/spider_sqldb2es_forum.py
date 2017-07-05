# -*- coding: utf-8 -*-

#v3: 需要利用db中的Imported字段


#import json
from u3d_spider.db_mysql.DBController import DBController
from u3d_spider.spider_utilities import store_json_with_list,timestamp_datetime
from elasticsearch import Elasticsearch

import time


g_fetch_comments=False

g_dbc=DBController(host='localhost', db_user_name='root', psd='',
                                   port=3306, db_name='uwa_qa_spider')

def fetch_posts(tbl_name,post_id_list,post_list):
    first_post_id=-1
    cur_id=-1
    for post_id in post_id_list:
        cur_id=cur_id+1
        row = g_dbc.execute_SQL(
            "SELECT Id, PostNumber, CreationTime, UpdateTime, Body, Staff FROM "+tbl_name+" WHERE Id=%s",(post_id))
        if row <= 0:
            print "Exception: canot find posts"
            return False
        post_db = g_dbc.cursor.fetchone()
        post_doc = {
            "post_number": int(post_db[1]),
            "creation_time": post_db[2],
            "update_time": post_db[3],
            "body": post_db[4],
            "is_staff":True if post_db[5] == "True" else False
        }
        if post_doc["post_number"] == 1: #post编号从1开始
            first_post_id=cur_id
        post_list.append(post_doc)
    return post_list[first_post_id]["body"]




def transdata(tbl_dict):

    try:
        es = Elasticsearch()
        #notdoanything=""
    except Exception,e:
        print "es open error"
        return

    fetch_step=1000

    sql_str = "SELECT Id, Link, Title, CreationTime, UpdateTime, ReplyCount, ViewCount, Tags, PostsId FROM " + tbl_dict[
                  "tbl_disc"] + " WHERE Imported='False' ORDER BY Id LIMIT %s "
    row = g_dbc.execute_SQL(sql_str, fetch_step)

    index_for_print=0
    while row > 0:
        doc_list=[]
        data_list= g_dbc.cursor.fetchall()
        for data in data_list:
            #print data

            tag_list=[]
            post_id_list=[]
            post_list=[]
            if data[7]:
                tag_list=data[7].split(',')
            if data[8]:
                post_id_list=data[8].split(',')

            doc ={
                "discussion_id":data[0],
                "link":data[1],
                "title":data[2],
                "creation_time": data[3],
                "update_time": data[4],
                "reply_count":data[5],
                "view_count":data[6],
                "source":tbl_dict["source"],
            }
            doc["tags"]=tag_list
            doc["tags_analyzer"] = tag_list

            if post_id_list ==[]:
                print "posts=[]"
                g_dbc.execute_SQL("UPDATE " + tbl_dict["tbl_discussion"] + " SET Imported = %s WHERE Id = %s",
                                  ("True", doc["discussion_id"]))
                continue

            doc["body"] = fetch_posts(tbl_dict["tbl_post"], post_id_list, post_list)
            doc["posts"]=post_list


            #print doc
            print index_for_print
            index_for_print=index_for_print+1
            action = {"index": {"_index":tbl_dict['es_index'], "_type": tbl_dict["es_type"], "_id":doc["discussion_id"] }}
            doc_list.append(action)
            doc_list.append(doc)

            #add: store doc 2 es
            res = es.index(index=tbl_dict['es_index'], doc_type=tbl_dict["es_type"], id=doc["discussion_id"], body=doc)
            print(res['created'])
            g_dbc.execute_SQL("UPDATE " + tbl_dict["tbl_discussion"] + " SET Imported = %s WHERE Id = %s",
                          ("True", doc["discussion_id"]))

        #changed: deprecated not write 2 json now
        #store_json_with_list(file_path,doc_list,"a")
        sql_str = "SELECT Id, Link, Title, CreationTime, UpdateTime, ReplyCount, ViewCount, Tags, PostsId FROM " + tbl_dict[
                  "tbl_disc"] + " WHERE Imported='False' ORDER BY Id LIMIT %s"
        row = g_dbc.execute_SQL(sql_str, fetch_step)
    print "complete"



if __name__=='__main__':

    tbl_u3dforum_dict ={"tbl_disc": "view_u3dforum_disc",
                        "tbl_post": "tbl_u3dforum_post",
                        "tbl_discussion":"tbl_u3dforum_discussion",
                           "es_index":"spider_u3d_forum",
                           "es_type": "discussion_u3dforum",
                           "source":"u3dforum"
    }
    import_date = timestamp_datetime((time.time()),"%Y%m%d%H%M%S")
    #g_file_path = g_file_path+import_date+tbl_u3danswers_dict["es_index"]+".json"
    transdata(tbl_u3dforum_dict)


