# -*- coding: utf-8 -*-

#v3: 需要利用db中的Imported字段


#import json
from u3d_spider.db_mysql.DBController import DBController
from u3d_spider.spider_utilities import store_json_with_list,timestamp_datetime
from elasticsearch import Elasticsearch

import time


g_fetch_comments = False

g_file_path="DBdata_u3danswers"
g_dbc=DBController(host='localhost', db_user_name='root', psd='',
                                   port=3306, db_name='uwa_qa_spider')


def fetch_all_comments(question_id,comment_list,comment_tbl_name1,comment_tbl_name2): #获取postId下的所有comment，并加入到comment_list中
    row = g_dbc.execute_SQL("SELECT CommentsId FROM %s WHERE PostId = %s"%(comment_tbl_name1, question_id))
    if row <= 0:
        print "Exception: comment count wrong"
        return
    comment_id_list = g_dbc.cursor.fetchone()[0].split(",")
    for comment_id in comment_id_list:
        row = g_dbc.execute_SQL("SELECT Text, CreationTime FROM %s WHERE Id=%s"%(comment_tbl_name2,comment_id))
        if row <= 0:
            print "Exception: cannot find comment"
            return
        comment_db = g_dbc.cursor.fetchone()
        comment_doc = {
            "body": comment_db[0],
            "creation_time": comment_db[1]
        }
        if comment_doc:
            comment_list.append(comment_doc)


def fetch_answer(accept_answer_id,tbl_name,is_accepted=False):
    row = g_dbc.execute_SQL(
        "SELECT Id, IsAccepted, CreationTime, Score, Body, CommentCount FROM "+tbl_name+" WHERE Id=%s",(accept_answer_id))
    if row <= 0:
        print "Exception: canot find accepted answer"
        return False
    answer_db = g_dbc.cursor.fetchone()
    answer_doc = {
        "is_accepted": is_accepted,
        "creation_time": answer_db[2],
        "score": answer_db[3], #可能存在问题，爬取时网页信息不全
        "body": answer_db[4]
    }
    #todo:comment
    answer_comment_list = []
    #if g_fetch_comments and answer_db[5] > 0:
    #    fetch_all_comments(accept_answer_id, answer_comment_list,comment_tbl_name1,comment_tbl_name2)
    #answer_doc["comments"] = answer_comment_list
    return answer_doc



def transdata(tbl_dict, file_path):

    try:
        es = Elasticsearch()
    except Exception,e:
        print "es open error"
        return

    fetch_step=1000

    sql_str = "SELECT Id, IsAccepted, AcceptAnswerId, CreationTime, UpdateTime, Score, ViewCount, AnswerCount, CommentCount, FollowingCount," \
              "Title, Body, Tags, AnswersId, Link, Version, Section, SectionLink FROM " + tbl_dict[
                  "tbl_qa"] + " WHERE Imported='False' AND PostType = 'question' ORDER BY Id LIMIT %s "
    row = g_dbc.execute_SQL(sql_str, fetch_step)
    index_for_print=0
    while row > 0:
        doc_list=[] #deprecated

        data_list= g_dbc.cursor.fetchall()
        for data in data_list:
            #print data
            accept_answer_id=data[2]
            is_accepted=False #数据库中的IsAccepted不准（只起到is_answered作用）
            if accept_answer_id and (not accept_answer_id == "NULL"): #!!Becareful!!!! u"NULL"  注意，使用NULL作为where条件会极大降低sql效率
                is_accepted=True
                if not data[1]=="True":
                    print "ue: is_accepted not equal"
            tag_list=[]
            answer_list=[]
            answer_id_list=[]
            comment_list=[]
            if data[12]:
                tag_list = data[12].split(',')
            if data[13]:
                answer_id_list = data[13].split(',')
            # db_u3danswers中的replycount包含了comment，所以与answer count并不一定对应 len(answer_id_list)
            doc ={
                "question_id":data[0],
                "is_accepted":is_accepted,
                "creation_time": data[3], #stored error before 2017-06-01
                "update_time": data[4],
                "score":data[5],
                "view_count":data[6],
                "answer_count":len(answer_id_list),
                "comment_count":data[8],
                "favorite_count":data[9],
                "title":data[10],
                "body":data[11],
                "source":tbl_dict["source"],
                "link":data[14],
                "version":data[15],
                "section":data[16],
                "section_link":data[17]
            }
            doc["tags"]=tag_list
            doc["tags_analyzer"] = tag_list
            #todo:comments
            #if(g_fetch_comments and doc["comment_count"]>0):
            #    fetch_all_comments(doc["question_id"], comment_list,tbl_dict["tbl_comment_post"],tbl_dict["tbl_comment"])
            #doc["comments"]=comment_list


            #accepted answer
            if is_accepted:
                answer_doc= fetch_answer(accept_answer_id, tbl_dict["tbl_post"],True)
                if answer_doc:
                    answer_list.append(answer_doc)

            #if not data[7] == len(answer_id_list):
            #    print "reply count inconsistent with answer count, question_id: "+data[0]


            # answers
            for answer_id in answer_id_list:
                if answer_id == accept_answer_id:
                    continue
                answer_doc=fetch_answer(answer_id, tbl_dict["tbl_post"])
                if answer_doc:
                    answer_list.append(answer_doc)

            doc["answers"]=answer_list
            #print doc
            print index_for_print
            index_for_print=index_for_print+1
            action = {"index": {"_index":tbl_dict['es_index'], "_type": tbl_dict["es_type"], "_id":doc["question_id"] }}
            doc_list.append(action)
            doc_list.append(doc)
            #add: store doc 2 es
            res = es.index(index=tbl_dict['es_index'], doc_type=tbl_dict["es_type"], id=doc["question_id"], body=doc)
            print(res['created'])
            if not res['created']=="True":
                print "create failed"
            g_dbc.execute_SQL("UPDATE " + tbl_dict["tbl_post"] + " SET Imported = %s WHERE Id = %s",
                              ("True", doc["question_id"]))

        #changed: deprecated not write 2 json now
        #store_json_with_list(file_path,doc_list,"a")
        sql_str = "SELECT Id, IsAccepted, AcceptAnswerId, CreationTime, UpdateTime, Score, ViewCount, AnswerCount, CommentCount, FollowingCount," \
                  "Title, Body, Tags, AnswersId, Link, Version, Section, SectionLink FROM " + tbl_dict[
                      "tbl_qa"] + " WHERE Imported='False' AND PostType = 'question' ORDER BY Id LIMIT %s "
        row = g_dbc.execute_SQL(sql_str, fetch_step)
    print "complete"



if __name__=='__main__':
    tbl_ueanswers_dict = {"tbl_qa": "tbl_ueanswers_post",
                           "tbl_post": "tbl_ueanswers_post",
                           #"tbl_comment_post": "view_u3danswers_post_comments",
                           #"tbl_comment": "tbl_u3danswers_comment",
                           #"ff_count":"FollowingCount",
                           "es_index":"ue_spider_answers",
                           "es_type": "question_ueanswers",
                           "source":"ueanswers"}

    import_date = timestamp_datetime((time.time()),"%Y%m%d%H%M%S")


    g_file_path = g_file_path+import_date+tbl_ueanswers_dict["es_index"]+".json"
    transdata(tbl_ueanswers_dict,g_file_path)


