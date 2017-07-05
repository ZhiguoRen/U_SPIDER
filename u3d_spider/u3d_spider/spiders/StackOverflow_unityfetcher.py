# -*- coding: utf-8 -*-

from stackapi import StackAPI
from u3d_spider.spider_utilities import store_json
from u3d_spider.spider_utilities import load_json
from u3d_spider.spider_utilities import datetime_timestamp
from u3d_spider.db_mysql.DBController import DBController
from u3d_spider.spider_utilities import timestamp_datetime
from collections import deque
import json

g_file_path="tempStackFetchingState.json"
g_dbc = DBController(host='localhost', db_user_name='root', psd='',
                                   port=3306, db_name='uwa_qa_spider')
g_stack_filter='!WzMsMbzXivo6ssptMFZ0P(hrbEKf0WqvhAlWAgX'


can_stop_when_find_fetched=False

g_file_fix_path="tempStackFetchingState4Fix.json"
g_fix_info=True
g_fix_this=True

def write_question2db():
    foo=""

def write_fetchstate2json():
    foo=""

def need_fetch(question_id, update_time):

    sql_str="SELECT UpdateTime FROM tbl_stack_post WHERE Id = %s" %question_id
    row = g_dbc.cursor.execute(sql_str)
    if row <= 0:
        return True

    last_up_date_time= g_dbc.cursor.fetchone()[0]
    last_up_date_time= datetime_timestamp(last_up_date_time)
    if last_up_date_time < update_time:
        return True
    else:
        return False

def fetch_questions_fix_info():
    SITE = StackAPI('stackoverflow')
    SITE.max_pages =1
    SITE.page_size=100

    #get state from tempjson
    state=load_json(g_file_fix_path)
    if state==None:
        state={"fetching_page":1,
               "has_more":True,
               "question_fetching_1st":True
        }
        store_json(g_file_fix_path,dict(state))

    #question_ids=[]
    #question_ids=deque(question_ids)
    state=dict(state)
    fetching_page=state["fetching_page"]
    question_fetching_1st=state["question_fetching_1st"]
    has_more = state["has_more"]
    quato_remaining=10

    while (quato_remaining>5): #and has_more:
        questions = SITE.fetch('questions',page=fetching_page, tagged='unity3d', order='desc', sort='activity', filter=g_stack_filter)
        post_item_list=questions["items"]
        found_fetched_question=False
        for post_item in post_item_list:
            write_item_2db4fix(post_item,"question")

        has_more=questions["has_more"]
        #store state2json
        state['fetching_page']=fetching_page
        state['has_more']=has_more
        if not has_more:
            state['question_fetching_1st']=False
            store_json(g_file_fix_path, dict(state))
            break
        store_json(g_file_fix_path, dict(state))

        print "page:" + str(questions["page"]-1)
        fetching_page = int(questions["page"])


def fetch_questions():
    SITE = StackAPI('stackoverflow')
    SITE.max_pages =1
    SITE.page_size=100

    #get state from tempjson
    state=load_json(g_file_path)
    if state==None:
        state={"fetching_page":1,
               "has_more":True,
               "question_fetching_1st":True
        }
        store_json(g_file_path,dict(state))

    #question_ids=[]
    #question_ids=deque(question_ids)
    state=dict(state)
    fetching_page=state["fetching_page"]
    question_fetching_1st=state["question_fetching_1st"]
    has_more = state["has_more"]
    quota_remaining=10

    if not question_fetching_1st:
        fetching_page=1

    while (quota_remaining>5): #and has_more:
        questions = SITE.fetch('questions',page=fetching_page,  filter=g_stack_filter, tagged='unity3d', order='desc', sort='activity')
        post_item_list=questions["items"]
        found_fetched_question=False
        for post_item in post_item_list:
            #compare last_activity with the db
            if not need_fetch(post_item['question_id'], post_item['last_activity_date']):
                found_fetched_question = True
                print "pass this question"
                continue
            write_item_2db(post_item,"question")
            print "write 2 db"

        has_more=questions["has_more"]
        #store state2json
        state['fetching_page']=fetching_page
        #state['has_more']=has_more
        quota_remaining=questions['quota_remaining']
        if not has_more:
            state['question_fetching_1st']=False
            state['last_fetched_page'] = str(questions["page"]-1)
            store_json(g_file_path, dict(state))
            break
        store_json(g_file_path, dict(state))

        if found_fetched_question:
            print "found fetched question"
            if can_stop_when_find_fetched:
                if not question_fetching_1st:
                    break
        else:
            print "new page" + str(questions["page"]-1)
        print "page:" + str(questions["page"]-1)
        fetching_page = int(questions["page"])




def write_item_2db(post_item,post_type):

    # common body
    title = post_item['title']
    body = post_item['body']
    creation_date = post_item['creation_date']
    creation_date = timestamp_datetime(creation_date)
    comment_count = post_item['comment_count']
    last_activity_date = post_item['last_activity_date']
    last_activity_date = timestamp_datetime(last_activity_date)
    score = post_item['score']

    question_id = post_item['question_id']

    #id
    id=question_id
    if post_type == "answer":
        id=post_item['answer_id']

    sql_update_post = "INSERT INTO tbl_stack_post (Id, PostType, CreationTime, UpdateTime, Score, CommentCount, Title, Body, Imported) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)" \
                          "ON DUPLICATE KEY UPDATE Id=VALUES(Id), PostType=VALUES(PostType), CreationTime=VALUES(CreationTime), " \
                      "UpdateTime=VALUES(UpdateTime), Score=VALUES(Score), CommentCount=VALUES(CommentCount), " \
                      "Title=VALUES(Title), Body=VALUES(Body), Imported=VALUES(Imported)"
    g_dbc.execute_SQL(sql_update_post,(id,post_type,creation_date,last_activity_date,score,comment_count,title,body,"False"))


    #special for answer and question
    if post_type == "answer":
        is_accepted = str(post_item['is_accepted'])
        sql_update_answer_post="UPDATE tbl_stack_post SET IsAccepted = %s, ParentId = %s WHERE Id = %s"
        g_dbc.execute_SQL(sql_update_answer_post, (is_accepted, question_id, id))

    elif post_type == "question":
        view_count= post_item['view_count']
        favorite_count= post_item['favorite_count']

        # tags
        for tag in post_item['tags']:
            tag_name = tag
            tag_id = tag
            sql_update_tag = "INSERT INTO tbl_stack_tag (Id, Name) VALUES (%s, %s)" \
                             "ON DUPLICATE KEY UPDATE Name=VALUES(Name)"
            g_dbc.execute_SQL(sql_update_tag, (tag_id, tag_name))

            # tag_question
            sql_find_tag_question = "SELECT * FROM tbl_stack_post_tag WHERE PostId = %s AND TagId = %s"
            count = g_dbc.cursor.execute(sql_find_tag_question, (question_id, tag_id))
            if count == 0:
                sql_insert_tag_question = "INSERT INTO tbl_stack_post_tag VALUES(%s, %s)"
                g_dbc.cursor.execute(sql_insert_tag_question, (question_id, tag_id))
                g_dbc.conn.commit()
                print "insert tag-post: post: " + str(question_id) + " tag: " + tag_id


        is_answered = str(post_item['is_answered'])
        answer_count = post_item['answer_count']

        # answers
        if answer_count>0:
            for answer_item in post_item['answers']:
                write_item_2db(answer_item, "answer")

        sql_update_question_post="UPDATE tbl_stack_post SET ViewCount= %s, FavoriteCount = %s, AnswerCount= %s, IsAccepted= %s  WHERE Id = %s"
        g_dbc.cursor.execute(sql_update_question_post,(view_count, favorite_count, answer_count,is_answered,id))
        g_dbc.conn.commit()
        #if is_answered == "True": #is_answered和存在accepted answer并不相同
        if 'accepted_answer_id' in post_item.keys():
            accepted_answer_id = post_item['accepted_answer_id']
            g_dbc.execute_SQL("UPDATE tbl_stack_post SET AcceptAnswerId= %s WHERE Id= %s",(accepted_answer_id,id))


    # comments
    if comment_count>0:
        for comment in post_item['comments']:
            comment_id = comment['comment_id']
            body = comment['body']
            creation_date = timestamp_datetime(comment['creation_date'])
            post_id = comment['post_id']
            post_type = comment['post_type']
            score = comment['score']
            sql_update_comment = "INSERT INTO tbl_stack_comment (Id, PostId, PostType, Score, Text, CreationTime) VALUES (%s, %s, %s, %s, %s, %s)" \
                             "ON DUPLICATE KEY UPDATE Id=VALUES(Id), PostId=VALUES(PostId), PostType=VALUES(PostType), Score=VALUES(Score), Text=VALUES(Text), CreationTime=VALUES(CreationTime)"
            g_dbc.execute_SQL(sql_update_comment, (comment_id, post_id, post_type, score, body, creation_date))




def write_item_2db4fix(post_item,post_type):
    # common body
    question_id = post_item['question_id']

    '''    title = post_item['title']
    body = post_item['body']
    creation_date = post_item['creation_date']
    creation_date = timestamp_datetime(creation_date)
    comment_count = post_item['comment_count']
    last_activity_date = post_item['last_activity_date']
    last_activity_date = timestamp_datetime(last_activity_date)
    score = post_item['score']
'''
    #id
    id=question_id
    if post_type == "answer":
        id=post_item['answer_id']

    #special for answer and question
    if post_type == "answer":
        is_accepted = str(post_item['is_accepted'])
        sql_update_answer_post="UPDATE tbl_stack_post SET IsAccepted = %s, ParentId = %s WHERE Id = %s"
        g_dbc.cursor.execute(sql_update_answer_post, (is_accepted, question_id, id))
        #fix
        g_dbc.conn.commit()
        print "fix answer id:"+str(id)

    elif post_type == "question":
        view_count= post_item['view_count']
        favorite_count= post_item['favorite_count']
        is_answered = str(post_item['is_answered'])
        answer_count = post_item['answer_count']

        # answers
        if answer_count>0:
            for answer_item in post_item['answers']:
                write_item_2db4fix(answer_item, "answer")

        sql_update_question_post="UPDATE tbl_stack_post SET ViewCount= %s, FavoriteCount = %s, AnswerCount= %s, IsAccepted= %s WHERE Id = %s"
        g_dbc.cursor.execute(sql_update_question_post,(view_count, favorite_count, answer_count,is_answered,id))
        #fix
        g_dbc.conn.commit()
        print "fix question id:" + str(id)
        #if is_answered == "True": #is_answered和存在accepted answer并不相同
        if 'accepted_answer_id' in post_item.keys():
            accepted_answer_id = post_item['accepted_answer_id']
            g_dbc.cursor.execute("UPDATE tbl_stack_post SET AcceptAnswerId= %s WHERE Id = %s",(accepted_answer_id, id))
            #fix
            g_dbc.conn.commit()
            print "fix question id:" + str(id)




def fetch_answers():
    foo=""

def fetch_comments():
    foo=""

#fetch_questions()
if __name__=="__main__":
    #fetch_questions_fix_info()
    fetch_questions()