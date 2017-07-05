# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exceptions import DropItem
from datetime import datetime
import json
#import pymongo
from scrapy import log
from scrapy.conf import settings
from db_mysql.DBController import DBController

from u3d_spider.spider_utilities import timestamp_datetime
import time
import os
#from scrapy.exporters import JsonItemExporter


#os.chdir()
class DropEarlyDataPipeline(object):
    def process_item(self, item, spider):
        # 检查item更新日期是否一年以内
        update_at = item['update_time']
        try:
            update_at = datetime.strptime(update_at, settings['DATE_FORMAT'])
        except:
            print("Date format error - update_at %s"%update_at)
            raise
        delta = datetime.now()-update_at
        if delta.days > 365:
            raise DropItem("数据更新日期过早  %s" % item)       
        return item


class DuplicatesPipeline(object):
    def __init__(self):
        self.ids_seen = set()
        self.file = 'data/question_list.jl'

    def open_spider(self, spider):
        self.exist_questions = {}
        with open(self.file, 'r') as f:
            for line in f:
                item = json.loads(line)
                self.exist_questions[item['question_id']] = item['update_time']


        

    def process_item(self, item, spider):
        # 检查id是否刚刚处理过
        if item['question_id'] in self.ids_seen:
            raise DropItem('数据刚刚处理过 %s' % item)
        else:
            self.ids_seen.add(item['question_id'])

        # 检查item是否存在且未改动
        if self.exist_questions.get(item['question_id']) == item['update_time']:
             # 放弃数据
            raise DropItem("数据已存在且未改动 %s" % item)

        return item


class JsonWriterPipeline(object):
    def __init__(self):
        self.file = open('data/update_question_list.jl', 'w')

    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        #print line
        return item

    def close_spider(self, spider):
        self.file.close()


class Json4ESWriterPipeline(object):
    def __init__(self):
        import_date = timestamp_datetime((time.time()), "%Y%m%d%H%M%S")
        file_path = 'ue_answers' + import_date + ".json"
        self.json_file = open(file_path, 'a')
        self.current_write_doc_id = 0

    def process_item(self, item, spider):
        if spider.name == "ue_answers_spider":
            accept_answer_id = item['posts'][0]["accept_id"]
            is_accepted = False  # 数据库中的IsAccepted不准（只起到is_answered作用）
            if accept_answer_id and (
            not accept_answer_id == "NULL"):  # !!Becareful!!!! u"NULL"  注意，使用NULL作为where条件会极大降低sql效率
                is_accepted = True

            tag_list = []
            answer_list = []
            for tag in item["tags"]:
                tag_list.append(tag["tag"])

            for i, post in enumerate(item["posts"]):
                if i == 0:
                    continue
                answer_doc = {
                    "is_accepted": is_accepted,
                    "creation_time": post["creation_time"],
                    "score": post['score'],
                    "body": post['body']
                }
                answer_list.append(answer_doc)

            # todo: comments
            comment_list = []

            # db_u3danswers中的replycount包含了comment，所以与answer count并不一定对应 len(answer_id_list)
            doc = {
                "question_id": item["discussion_id"],
                "is_accepted": is_accepted,
                "creation_time": item["posts"][0]["creation_time"],
                "update_time": item["update_time"],
                "score": item["posts"][0]["score"],
                "view_count": item["view_count"],
                "answer_count": len(answer_list),
                "comment_count": 0,
                "favorite_count": item["posts"][0]["following_count"],
                "title": item["title"],
                "body": item["posts"][0]["body"],
                "source": "ueanswers",
                "link": item["link"]
            }

            doc["answers"] = answer_list
            doc["tags"] = tag_list
            doc["tags_analyzer"] = tag_list

            # print doc
            action = {"index": {"_index": "spider_ue", "_type": "question_uedanswers", "_id": doc["question_id"]}}
            self.json_file.write(json.dumps(dict(action))+"\n")
            self.json_file.write(json.dumps(dict(doc))+"\n")
            self.current_write_doc_id = self.current_write_doc_id + 1
            if True:#self.current_write_doc_id %100 ==0:
                self.json_file.flush()
            #print line
            item["other_info"]["writed2json"]="True"
        return item

    def close_spider(self, spider):
        self.json_file.flush()
        self.json_file.close()



class MySQLDBPipeline(object):
    def __init__(self):
        # connect to database
        self.dbc = DBController(host=settings['MYSQL_SERVER'],
                                db_user_name=settings['MYSQL_USER'],
                                psd=settings['MYSQL_PSW'],
                                port=settings['MYSQL_PORT'],
                                db_name=settings['MYSQL_DB'])

        # question-list spider 参数设置
        self.current_update_date = datetime.now()


        #update_dates = self.updates.find()
        #if update_dates.count() > 0:
            # 这里需要手动调整last_update_date, 因为爬取过程中的失败会导致中断后重新开始
        #    self.last_update_date = update_dates.sort('update_at', pymongo.DESCENDING)[0]['update_at']  # 最近一次更新时间
            # self.last_update_date = update_dates.sort('update_at', pymongo.ASCENDING)[0]['update_at']  # 最早一次更新时间
        #else:

        self.last_update_date = datetime.strptime(settings['START_DATE'], settings['DATE_FORMAT'])

    def open_spider(self, spider):
        #log.msg("设置spider的last_update_date: %s" % self.last_update_date)
        #spider.set_last_update_date(self.last_update_date)
        todo="todo"


    def process_item(self, item, spider):
        #return item
        if not item:
            return item

        if spider.name == "u3dforum_spider":
            try:
                # 1. 更新tbl_u3dforum_discussion
                sql_update_question =  "INSERT INTO tbl_u3dforum_discussion (Id, Link, Title, CreationTime, UpdateTime, ReplyCount, ViewCount) VALUES (%s, %s, %s, %s, %s, %s, %s)" \
                                      "ON DUPLICATE KEY UPDATE Title=VALUES(Title), UpdateTime=VALUES(UpdateTime), ReplyCount=VALUES(ReplyCount), ViewCount=VALUES(ViewCount)"
                self.dbc.execute_SQL(sql_update_question, (
                item['discussion_id'], item['link'], item['title'], item['creation_time'], item['update_time'], item['reply_count'],
                item['view_count']))
                #self.dbc.conn.commit()

                #2. 更新tag
                for tag in item['tags']:
                    sql_update_tag = "INSERT INTO tbl_u3dforum_tag (Id, Link, Name) VALUES (%s, %s, %s)" \
                                     "ON DUPLICATE KEY UPDATE Name=VALUES(Name)"
                    self.dbc.execute_SQL(sql_update_tag,(tag['tag_id'], tag['link'], tag['tag']))
                    #self.dbc.conn.commit()
                    #3. 更新discussion_tag
                    sql_find_tag_question = "SELECT * FROM tbl_u3dforum_discussion_tag WHERE DiscussionId = %s AND TagId = %s"
                    count = self.dbc.cursor.execute(sql_find_tag_question,(item['discussion_id'],tag['tag_id']))
                    if count == 0:
                        sql_insert_tag_question = "INSERT INTO tbl_u3dforum_discussion_tag VALUES(%s, %s)"
                        self.dbc.execute_SQL(sql_insert_tag_question,(item['discussion_id'], tag['tag_id']))
                        #self.dbc.conn.commit()
                        print "insert u3dforum_discussion_tag: discussion: "+item['discussion_id']+" tag: "+tag['tag_id']

                # 4. 更新post
                for post in item['posts']:
                    sql_update_content = "INSERT INTO tbl_u3dforum_post (Id, DiscussionId, PostNumber, CreationTime, UpdateTime, Body, Staff) VALUES (%s, %s, %s, %s, %s, %s, %s)" \
                                         "ON DUPLICATE KEY UPDATE PostNumber=VALUES(PostNumber), UpdateTime=VALUES(UpdateTime), Body = VALUES(Body)"
                    self.dbc.execute_SQL(sql_update_content, (
                        post['post_id'], post['parent_id'], post['post_number'], post['creation_time'], post['update_time'], post['body'], post['is_staff']))
                    #self.dbc.conn.commit()

                self.dbc.execute_SQL("UPDATE tbl_u3dforum_spider_state SET LastDiscussionId = %s WHERE BaseTag = %s",(item['discussion_id'],item['tags'][0]['tag_id']))
                #self.dbc.conn.commit()
                log.msg("Discussion %s updated." % item['discussion_id'], level=log.DEBUG, spider=spider)
            except Exception,e:
                print e
        elif spider.name == "u3danswers_spider":
            try:
                #1 更新post
                for post in item["posts"]:
                    #question
                    if post['post_type'] == "question":
                        sql_update_question = "INSERT INTO tbl_u3danswers_post (Id, PostType, IsAccepted, AcceptAnswerId, UpdateTime, Score, AnswerCount, FollowingCount, Title, Body, Link, Imported) VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
                          "ON DUPLICATE KEY UPDATE Id=VALUES(Id), PostType=VALUES(PostType), IsAccepted =VALUES(IsAccepted), AcceptAnswerId=VALUES(AcceptAnswerId), " \
                      "UpdateTime=VALUES(UpdateTime), Score=VALUES(Score), AnswerCount=VALUES(AnswerCount), FollowingCount=VALUES(FollowingCount), " \
                      "Title=VALUES(Title), Body=VALUES(Body), Imported = VALUES(Imported)"
                        self.dbc.execute_SQL(sql_update_question,(post['post_id'], post['post_type'], "False" if post['accept_id'] =="NULL" else "True",
                                                                  post['accept_id'],
                                                                  post['update_time'], post['score'], item['reply_count'], post['following_count'],
                                                                  item['title'], post['body'],item['link'], "False"))
                    #answer
                    else:
                        sql_update_question = "INSERT INTO tbl_u3danswers_post (Id, PostType, IsAccepted, Score, Title, Body, ParentId, Link) VALUES (%s, %s,%s, %s, %s, %s, %s, %s)" \
                                              "ON DUPLICATE KEY UPDATE Id=VALUES(Id), PostType=VALUES(PostType), IsAccepted =VALUES(IsAccepted)," \
                                              "Score=VALUES(Score), " \
                                              "Title=VALUES(Title), Body=VALUES(Body)"
                        self.dbc.execute_SQL(sql_update_question,(post['post_id'], post['post_type'], "False" if post['accept_id'] =="NULL" else "True",
                                                                  post['score'],item['title'], post['body'],post['parent_id'], item['link']))

                #更新tag
                        # 2. 更新tag
                for tag in item['tags']:
                    sql_update_tag = "INSERT INTO tbl_u3danswers_tag (Id, Name, Link) VALUES (%s, %s, %s)" \
                                     "ON DUPLICATE KEY UPDATE Name=VALUES(Name)"
                    self.dbc.execute_SQL(sql_update_tag, (tag['tag_id'], tag['tag'], tag['link']))

                    # 3. 更新tag_question
                    sql_find_tag_question = "SELECT * FROM tbl_u3danswers_post_tag WHERE PostId = %s AND TagId = %s"
                    count = self.dbc.cursor.execute(sql_find_tag_question, (item['discussion_id'], tag['tag_id']))
                    if count == 0:
                        sql_insert_tag_question = "INSERT INTO tbl_u3danswers_post_tag VALUES(%s, %s)"
                        self.dbc.execute_SQL(sql_insert_tag_question, (item['discussion_id'], tag['tag_id']))
                        print "insert tag-question: question: " + item['discussion_id'] + " tag: " + tag['tag_id']
                #更新state
                self.dbc.execute_SQL("INSERT INTO tbl_u3danswers_spider_state (ScrapingTime, QuestionId, Link, QuestionUpdateTime, Finish) VALUES(%s, %s, %s, %s, %s)",
                                        (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),item['discussion_id'], item['link'], item['update_time'], 'False'))

            except Exception,e:
                print e
        elif spider.name == "ue_answers_spider":
            if item['other_info']['process_type']=="Insert or Update":
                self.insert_or_update_UE_questions(item)
            elif item['other_info']['process_type']=="Fix or Update":
                self.fix_or_update_UE_questions(item)

            '''
            try:
                #1 更新post
                for post in item["posts"]:
                    #question
                    if post['post_type'] == "question":

                        # Tags
                        tags=""
                        for tag in item["tags"]:
                            if tags=="":
                                tags=tag["tag"]
                            else:
                                tags = tags+","+tag["tag"]
                        # AnswersId
                        answersId=""
                        for asr in item["posts"]:
                            if asr['post_type'] == "question":
                                continue
                            if answersId=="":
                                answersId=asr["post_id"]
                            else:
                                answersId =answersId+","+asr["post_id"]

                        sql_update_question = "INSERT INTO tbl_ueanswers_post (Id, PostType, IsAccepted, AcceptAnswerId, CreationTime, UpdateTime, Score, ViewCount, AnswerCount, FollowingCount, Title, Body, Link, Tags, AnswersId, IsStaff,Version, Section, Imported) VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
                          "ON DUPLICATE KEY UPDATE Id=VALUES(Id), PostType=VALUES(PostType), IsAccepted =VALUES(IsAccepted), AcceptAnswerId=VALUES(AcceptAnswerId), " \
                      "UpdateTime=VALUES(UpdateTime), Score=VALUES(Score), AnswerCount=VALUES(AnswerCount), FollowingCount=VALUES(FollowingCount), " \
                      "Title=VALUES(Title), Body=VALUES(Body),Tags=VALUES(Tags), AnswersId=VALUES(AnswersId), Imported = VALUES(Imported)"
                        self.dbc.execute_SQL(sql_update_question,(post['post_id'], post['post_type'], "False" if post['accept_id'] =="NULL" else "True",
                                                                  post['accept_id'], post['creation_time'],
                                                                  post['update_time'], post['score'],item['view_count'], item['reply_count'], post['following_count'],
                                                                  item['title'], post['body'],item['link'],tags, answersId, post['is_staff'] ,item['other_info']['version'],item['other_info']['section'],item['other_info']['imported']))
                    #answer
                    else:
                        sql_update_question = "INSERT INTO tbl_ueanswers_post (Id, PostType, IsAccepted,CreationTime, Score, Title, Body, ParentId, Link, Tags, IsStaff) VALUES (%s,%s, %s,%s,%s, %s, %s, %s, %s, %s, %s)" \
                                              "ON DUPLICATE KEY UPDATE Id=VALUES(Id), PostType=VALUES(PostType), IsAccepted =VALUES(IsAccepted)," \
                                              "Score=VALUES(Score), " \
                                              "Title=VALUES(Title), Body=VALUES(Body),IsStaff=VALUES(IsStaff)"
                        self.dbc.execute_SQL(sql_update_question,(post['post_id'], post['post_type'], "False" if post['accept_id'] =="NULL" else "True",
                                                                  post['creation_time'],post['score'],item['title'], post['body'],post['parent_id'], item['link'], tags,post['is_staff']))

                #更新tag
                        # 2. 更新tag
                for tag in item['tags']:
                    sql_update_tag = "INSERT INTO tbl_ueanswers_tag (Id, Name, Link) VALUES (%s, %s, %s)" \
                                     "ON DUPLICATE KEY UPDATE Name=VALUES(Name)"
                    self.dbc.execute_SQL(sql_update_tag, (tag['tag_id'], tag['tag'], tag['link']))

                    # 3. 更新tag_question
                    sql_find_tag_question = "SELECT * FROM tbl_ueanswers_post_tag WHERE PostId = %s AND TagId = %s"
                    count = self.dbc.cursor.execute(sql_find_tag_question, (item['discussion_id'], tag['tag_id']))
                    if count == 0:
                        sql_insert_tag_question = "INSERT INTO tbl_ueanswers_post_tag VALUES(%s, %s)"
                        self.dbc.execute_SQL(sql_insert_tag_question, (item['discussion_id'], tag['tag_id']))
                        print "insert tag-question: question: " + item['discussion_id'] + " tag: " + tag['tag_id']
                #更新state
                self.dbc.execute_SQL("INSERT INTO tbl_ueanswers_spider_state (ScrapingTime, QuestionId, Link, QuestionUpdateTime, Finish) VALUES(%s, %s, %s, %s, %s)",
                                        (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),item['discussion_id'], item['link'], item['update_time'], 'False'))

            except Exception,e:
                print e
            '''
        '''
        elif spider.name == "question-list":
            #print item['question_id']+item['link']+ item['state']+ item['title']+ item['update_time']
            #1. 更新tbl_question
            # 因为foreign key，所以不能用REPLACE（先删除，后insert）
            sql_update_question = "INSERT INTO tbl_question (question_id, link, state, title, update_time, reply_num, following_num) VALUES (%s, %s, %s, %s, %s, %s, %s)" \
                                  "ON DUPLICATE KEY UPDATE state=VALUES(state), title=VALUES(title), update_time=VALUES(update_time), reply_num=VALUES(reply_num), following_num=VALUES(following_num)"
            self.dbc.cursor.execute(sql_update_question,(item['discussion_id'], item['link'], item['title'], item['update_time'], item['reply_num'], item['view_count']))
            self.dbc.conn.commit()

            # 2. 更新tag
            for tag in item['tags']:
                sql_update_tag = "INSERT INTO tbl_tag (tag_id, tag) VALUES (%s, %s)" \
                                 "ON DUPLICATE KEY UPDATE tag=VALUES(tag)"
                self.dbc.cursor.execute(sql_update_tag,(tag['tag_id'], tag['tag']))
                self.dbc.conn.commit()
                #3. 更新tag_question
                sql_find_tag_question = "SELECT * FROM tbl_question_tag WHERE question_id = %s AND tag_id = %s"
                count = self.dbc.cursor.execute(sql_find_tag_question,(item['question_id'],tag['tag_id']))
                if count == 0:
                    sql_insert_tag_question = "INSERT INTO tbl_question_tag VALUES(%s, %s)"
                    self.dbc.cursor.execute(sql_insert_tag_question,(item['question_id'], tag['tag_id']))
                    self.dbc.conn.commit()
                    print "insert tag-question: question: "+item['question_id']+" tag: "+tag['tag_id']

            #4. 更新content
            for content in item['contents']:
                sql_update_content = "INSERT INTO tbl_content (content_id, content, question_id, vote) VALUES (%s, %s, %s, %s)" \
                                     "ON DUPLICATE KEY UPDATE content=VALUES(content), vote=VALUES(vote)"
                self.dbc.cursor.execute(sql_update_content,(content['content_id'], content['content'], item['question_id'], content['vote']))
                self.dbc.conn.commit()
                #5. 更新question-qstcontent
                if content['is_question'] == 'True':
                    sql_find_qstcontent_question = "SELECT * FROM tbl_question_qstcontent WHERE question_id = %s AND qstcontent_id = %s"
                    count=self.dbc.cursor.execute(sql_find_qstcontent_question,(item['question_id'], content['content_id']))
                    if count == 0:
                        sql_insert_qstcontent_question = "INSERT INTO tbl_question_qstcontent VALUES(%s, %s)"
                        self.dbc.cursor.execute(sql_insert_qstcontent_question, (item['question_id'], content['content_id']))
                        self.dbc.conn.commit()
                        print "insert tag-question: question: " + item['question_id'] + " content: " + content['content_id']
                #6. 更新question-answer
                elif content['is_answer'] == 'True':
                    sql_find_answer_question = "SELECT * FROM tbl_question_answer WHERE question_id = %s AND content_id = %s"
                    count = self.dbc.cursor.execute(sql_find_answer_question,(item['question_id'], content['content_id']) )
                    if count == 0:
                        sql_insert_answer_question =  "INSERT INTO tbl_question_answer VALUES(%s, %s)"
                        self.dbc.cursor.execute(sql_insert_answer_question, (item['question_id'], content['content_id']))
                        self.dbc.conn.commit()
                        print "insert answer-question: question: " + item['question_id'] + " answer: " + content['content_id']
                #todo: 更新comments
            #self.questions.replace_one({'nodeid': item['nodeid']}, dict(item), upsert=True)
            #self.updates.update_one({'update_at': self.current_update_date}, {'$addToSet': {'content': item['nodeid']}},
            #                        upsert=True)
            log.msg("Question %s updated." % item['question_id'], level=log.DEBUG, spider=spider)

            #不再return item，不再cmd显示，方便查看debug
            #return item
        '''

    def close_spider(self, spider):
        #self.connection.close()
        self.dbc.stop()

    def insert_or_update_UE_questions(self, item):
        scraping_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        try:
            # 1 更新post
            for post in item["posts"]:
                #for debug:
                if not post['creation_time']:
                    print "creation_time -,-"
                # question
                if post['post_type'] == "question":

                    # Tags
                    tags = ""
                    for tag in item["tags"]:
                        if tags == "":
                            tags = tag["tag"]
                        else:
                            tags = tags + "," + tag["tag"]
                    # AnswersId
                    answersId = ""
                    for asr in item["posts"]:
                        if asr['post_type'] == "question":
                            continue
                        if answersId == "":
                            answersId = asr["post_id"]
                        else:
                            answersId = answersId + "," + asr["post_id"]

                    sql_update_question = "INSERT INTO tbl_ueanswers_post (Id, PostType, IsAccepted, AcceptAnswerId, CreationTime, UpdateTime, Score, ViewCount, AnswerCount, FollowingCount, Title, Body, Link, Tags, AnswersId, IsStaff,Version, Section,SectionLink, Imported, ScrapyTime) VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)" \
                                          "ON DUPLICATE KEY UPDATE Id=VALUES(Id), PostType=VALUES(PostType), IsAccepted =VALUES(IsAccepted), AcceptAnswerId=VALUES(AcceptAnswerId), " \
                                          "UpdateTime=VALUES(UpdateTime), Score=VALUES(Score), AnswerCount=VALUES(AnswerCount), FollowingCount=VALUES(FollowingCount), " \
                                          "Title=VALUES(Title), Body=VALUES(Body),Tags=VALUES(Tags), AnswersId=VALUES(AnswersId), Imported = VALUES(Imported),ScrapyTime=VALUES(ScrapyTime)"
                    self.dbc.execute_SQL(sql_update_question, (
                    post['post_id'], post['post_type'], "False" if post['accept_id'] == "NULL" else "True",
                    post['accept_id'], post['creation_time'],
                    post['update_time'], post['score'], item['view_count'], item['reply_count'],
                    post['following_count'],
                    item['title'], post['body'], item['link'], tags, answersId, post['is_staff'],
                    item['other_info']['version'], item['other_info']['section'],item['other_info']['section_link'], item['other_info']['imported'],scraping_time))
                # answer
                else:
                    sql_update_question = "INSERT INTO tbl_ueanswers_post (Id, PostType, IsAccepted,CreationTime, Score, Title, Body, ParentId, Link, Tags, IsStaff, ScrapyTime) VALUES (%s,%s, %s,%s,%s, %s, %s, %s, %s, %s, %s,%s)" \
                                          "ON DUPLICATE KEY UPDATE Id=VALUES(Id), PostType=VALUES(PostType), IsAccepted =VALUES(IsAccepted)," \
                                          "Score=VALUES(Score), " \
                                          "Title=VALUES(Title), Body=VALUES(Body),IsStaff=VALUES(IsStaff),ScrapyTime=VALUES(ScrapyTime)"
                    self.dbc.execute_SQL(sql_update_question, (
                    post['post_id'], post['post_type'], "False" if post['accept_id'] == "NULL" else "True",
                    post['creation_time'], post['score'], item['title'], post['body'], post['parent_id'], item['link'],
                    tags, post['is_staff'],scraping_time))

                    # 更新tag
                    # 2. 更新tag
            for tag in item['tags']:
                sql_update_tag = "INSERT INTO tbl_ueanswers_tag (Id, Name, Link) VALUES (%s, %s, %s)" \
                                 "ON DUPLICATE KEY UPDATE Name=VALUES(Name)"
                self.dbc.execute_SQL(sql_update_tag, (tag['tag_id'], tag['tag'], tag['link']))

                # 3. 更新tag_question
                sql_find_tag_question = "SELECT * FROM tbl_ueanswers_post_tag WHERE PostId = %s AND TagId = %s"
                count = self.dbc.cursor.execute(sql_find_tag_question, (item['discussion_id'], tag['tag_id']))
                if count == 0:
                    sql_insert_tag_question = "INSERT INTO tbl_ueanswers_post_tag VALUES(%s, %s)"
                    self.dbc.execute_SQL(sql_insert_tag_question, (item['discussion_id'], tag['tag_id']))
                    print "insert tag-question: question: " + item['discussion_id'] + " tag: " + tag['tag_id']
            # 更新state
            self.dbc.execute_SQL(
                "INSERT INTO tbl_ueanswers_spider_state (ScrapingTime, QuestionId, Link, QuestionUpdateTime, Finish,ProcessType) VALUES(%s, %s, %s, %s, %s, %s)",
                (scraping_time, item['discussion_id'], item['link'],
                 item['update_time'], 'False',"Insert or Update"))

        except Exception, e:
            print e

    def fix_or_update_UE_questions(self, item):
        scraping_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        value_list=[]
        sql_update_set=""
        for (k,v) in item['other_info']['summury_info'].items():
            sql_update_set = sql_update_set + str(k) +" =  %s,"
            value_list.append(v)


        sql_str="UPDATE tbl_ueanswers_post SET "+sql_update_set+" ScrapyTime =%s WHERE Id = %s"
        value_list.append(scraping_time)
        value_list.append(item['discussion_id'])
        self.dbc.execute_SQL(sql_str, (tuple(value_list)))
        # 更新state
        self.dbc.execute_SQL(
            "INSERT INTO tbl_ueanswers_spider_state (ScrapingTime, QuestionId, Link, QuestionUpdateTime, Finish, ProcessType) VALUES(%s, %s, %s, %s, %s, %s)",
            (scraping_time, item['discussion_id'], item['link'],
             item['update_time'], 'False', "Fix or Update"))



'''
class MongoDBPipeline(object):
    def __init__(self):
        # connect to database
        self.connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = self.connection[settings['MONGODB_DB']]
        self.questions = db[settings['MONGODB_COLLECTION']['questions']]
        self.updates = db[settings['MONGODB_COLLECTION']['updates']]
        #self.contents = db[settings['MONGODB_COLLECTION']['contents']]

        # question-list spider 参数设置
        update_dates = self.updates.find()
        if update_dates.count() > 0:
            # 这里需要手动调整last_update_date, 因为爬取过程中的失败会导致中断后重新开始
            self.last_update_date = update_dates.sort('update_at', pymongo.DESCENDING)[0]['update_at']  # 最近一次更新时间
            #self.last_update_date = update_dates.sort('update_at', pymongo.ASCENDING)[0]['update_at']  # 最早一次更新时间
        else:
            self.last_update_date = datetime.strptime(settings['START_DATE'], settings['DATE_FORMAT'])
        
        self.current_update_date = datetime.now()

    def open_spider(self, spider):
        log.msg("设置spider的last_update_date: %s" % self.last_update_date)
        spider.set_last_update_date(self.last_update_date)

    def process_item(self, item, spider):
        self.questions.replace_one({'nodeid': item['nodeid']}, dict(item), upsert=True)
        self.updates.update_one({'update_at': self.current_update_date}, {'$addToSet': {'content': item['nodeid']}}, upsert=True)
        log.msg("Question %s updated."%item['nodeid'], level=log.DEBUG, spider=spider)

        return item

    def close_spider(self, spider):
        self.connection.close()
'''