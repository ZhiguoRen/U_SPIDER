# -*- coding: utf-8 -*-
import scrapy

from ..db_mysql.DBController import DBController
from ..items import TagItem, PostItem, DiscussionItem

from ..spider_utilities import timestamp_datetime,datetime_timestamp

from scrapy.conf import settings
from scrapy_splash import SplashRequest
import time


g_update_state_db = True #记录爬取状态
g_force_crawl = False    #True: 强制继续crwal， if发现未update问题  False: 发现超出stop_time后退出
g_force_update = False    #强制写db， if发现未update问题
g_fix = False
g_update_summury_info=True #修复summary 或者只更新 summary信息 利用other_info中的字段

g_update_summury_stop_time = datetime_timestamp("2017-06-01", "%Y-%m-%d")
g_stop_time = datetime_timestamp("2016-01-01", "%Y-%m-%d")
g_stop_num_threshhold = 25 #当发现多余threshhold个未更新的问题时，停止crawl(多线程原因)

class UEAnswersSpider(scrapy.Spider):
    name = "ue_answers_spider"
    allowed_domains = ["answers.unrealengine.com"]
    # 如爬取中断，查看log并修改这里的page id重新下载
    start_urls = [ "https://answers.unrealengine.com/index.html?page=1&pageSize=30&sort=active&tab=all-questions"]




    def __init__(self, *a, **kw):
        super(UEAnswersSpider, self).__init__(*a, **kw)
        self.finish_crawl=False
        #self.s_factor=100
        self.dbc = DBController(host=settings['MYSQL_SERVER'],
                                db_user_name=settings['MYSQL_USER'],
                                psd=settings['MYSQL_PSW'],
                                port=settings['MYSQL_PORT'],
                                db_name=settings['MYSQL_DB'])

    #def start_requests(self):
    #   yield SplashRequest("https://answers.unrealengine.com/index.html?tab=hottest&sort=hottest", callback=self.parse,
    #        args={ 'wait': 0.5, 'html': 1,'images':0,})





    def parse(self, response):   #负责生成翻页request，生成question_item request, 记录question_title中的概括信息
        if self.finish_crawl:
            print "ue_answers_spider finish crawl"
            yield

        url = response.url

        # 保存当前页面in db_state
        if g_update_state_db:
            sql_update_state_db = "INSERT INTO tbl_ueanswers_spider_state (ScrapingTime, QuestionId, Link, QuestionUpdateTime, Finish, ProcessType) VALUES (%s, %s, %s, %s, %s, %s)"
            self.dbc.execute_SQL(sql_update_state_db, (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),"NULL",url,"NULL","False","Process URL"))

        process_type = "Insert or Update"

        found_questions_num=0

        question_list = response.css('.short-summary')
        cur_priority = len(question_list) * 20 #+ self.s_factor
        for question in question_list:
            #item = QuestionItem()
            item = DiscussionItem()
            item['link'] = response.urljoin(question.css('h2>a::attr(href)').extract_first())
            #item['question_id'] = item['link'].split('/')[-2]
            item['discussion_id']=item['link'].split('/')[-2]
            item['title'] = question.css('h2>a::attr(title)').extract_first()
            update_time = question.css('.friendly-date ::attr(title)').extract_first()
            if not update_time:
                tst=question.css('.friendly-date').extract()
                print "update_time is None"
                #continue
                if item['title']:
                    print "Exception: update_time is None"
                break#会重复标签次，所以直接跳出


            #update_time = datetime.strptime(update_at, settings['DATE_FORMAT'])
            update_time = datetime_timestamp(update_time,"%Y, %b %d at %H:%M:%S+0000")  #timestamp now
            item['update_time'] = timestamp_datetime(update_time)  #str now
            item['posts'] = []
            item['state']='notAccepted'
            item['reply_count'] = int(question.css('.status>.item-count ::text').extract_first())

            thousand="normal view"
            if question.css(".question-stats .thousand"):
                thousand=question.css(".question-stats .thousand ::text").extract()
                item['view_count'] = int(float(thousand[1].split("k")[0])*1000)
                #count=float(count)
                #count=count*1000
                #item['score']=question.css(".question-stats>span ::text").extract_first().split(":")[1]
            #else:
            question_stats=question.css(".question-stats>span ::text").extract()
            for stats in question_stats:
                if stats.split(":")[0] == "Votes":
                    item['score']=stats.split(":")[1]
                elif thousand == "normal view" and stats.split(":")[0] == "Views":
                    item['view_count']=int(stats.split(":")[1])
            if item['view_count'] == 0:
                print "view_count == 0"

            if question.css('.answered-accepted').extract_first():
                print "state: accepted"
                item['state'] = 'Accepted'

            #结束判断1： 是否到达截至日期
            if update_time<=g_stop_time:
                print "can stop: update_time:"+str(update_time)+"   older then stop_time:"+str(g_stop_time)
                self.finish_crawl = True
                break

            # 结束判断2：为开启force_crawl并且 g_fix到达上限，并且发现同一页未更问题数到上限
            if not (g_fix or g_force_crawl):
                if (found_questions_num > g_stop_num_threshhold):
                    if not (g_update_summury_info and update_time >= g_update_summury_stop_time):
                        print "finish and break"
                        self.finish_crawl = True
                        break

            # other info
            other_info = {'process_type': process_type,
                          'section': question.css(".question-section>a ::text").extract_first(),
                          'section_link': response.urljoin(
                              question.css(".question-section>a ::attr(href)").extract_first()),
                          'version': question.css(".question-section>span ::text").extract_first().replace("\n","").strip(),
                          'writed2json': "False",  # deprecated
                          'imported': "False"}
            item['other_info'] = other_info


            row_count=0
            if not g_force_update:
                sql_find_update_time="SELECT UpdateTime,ViewCount FROM tbl_ueanswers_post WHERE Id = %s"
                row_count=self.dbc.execute_SQL(sql_find_update_time,item['discussion_id'])
            # 当force_update为False时，判断更新日期，若未更新，则pass
            if row_count > 0:
                data_row = self.dbc.cursor.fetchone()
                last_update_time = data_row[0]
                last_update_time = datetime_timestamp(last_update_time)
                #跳过判断：未更新
                if last_update_time >= update_time:
                    item['other_info']['last_view_count'] = data_row[1]
                    print "question not updated: "+item['discussion_id']
                    found_questions_num = found_questions_num+1
                    print "found question not updated num: "+ str(found_questions_num)
                    if (g_fix or g_update_summury_info):
                        if self.wrap_item_with_summury(item):
                            print "will fix or update summury info"
                            yield item
                        else:
                            print "skip fix"
                    else:
                        print "skip this question"
                    continue

                print "question updated:" +item['discussion_id'] +"  last_update_time-" + str(last_update_time) + "    update_time-" + str(update_time)
            else:
                print "found question: "+item['discussion_id']

            item['tags'] = []
            for t in question.css('.tags>a'):
                tag_item = TagItem()
                tag_item['link'] = response.urljoin(t.css('::attr(href)').extract_first())
                tag_item['tag_id'] = t.css('::text').extract_first()
                tag_item['tag'] = t.css('::text').extract_first()
                item['tags'].append(dict(tag_item))

            #yield item
            #生成request来爬取question内容
            yield scrapy.Request(response.urljoin(item['link']), meta={'item':item,
                                                                       'splash': {
                                                                           'args': {
                                                                               # set rendering arguments here]
                                                                               'html': 1,
                                                                               'timeout':60,
                                                                               #'images':0,
                                                                               # 'url' is prefilled from request url
                                                                               # 'http_method' is set to 'POST' for POST requests
                                                                               # 'body' is set to request body for POST requests
                                                                           },
                                                                           #'dont_process_response': True,
                                                                       # optional, default is False
                                                                           #'magic_response': False,
                                                                       # optional, default is True
                                                                       }
                                                                       }, callback=self.parse_question,priority=cur_priority)
            cur_priority=cur_priority-10

        #生成下一页的request
        next_page=None
        if not self.finish_crawl: #不再继续翻页
            for page in response.css('.pagination .pager-button'):
                if page.css('::attr(title)').extract_first() == "next":
                    next_page = page.css('::attr(href)').extract_first()
                    break

        if (next_page is not None):
            print "next page request"
            #self.s_factor=self.s_factor-len(question_list)
            yield scrapy.Request(response.urljoin(next_page), callback=self.parse, priority=-1)
        else:
            print "cannot  next pagefind，current page: "+ response.url


    def parse_question(self,response):
        item = response.meta['item']
        print "parse_question"
        #item['following_num'] = response.css('.question-follow-widget strong::text').extract_first()
        #print item['following_num']

        #posts:
        # item['posts'][0]为question，若len(item.post)>=1 则不再加入question
        #1. question
        if len(item['posts']) < 1:
            qst_post = PostItem()
            qst_post['post_type'] = 'question'
            qst_post['post_id'] = item['discussion_id']
            qst_post['update_time'] = item['update_time']
            qst_post['accept_id'] = "NULL"

            if not response.css("#question-table"):
                print "lost scrapy-splash, score of answers will not be correct"
                yield scrapy.Request(response.urljoin(item['link']), meta={'item': item,
                                                                           }, callback=self.parse_question,
                                     priority=10000)
                return
            qst_post['body']="scrapy failed"
            for tbody in response.css("#question-table"):
                if tbody.css(".question-body").extract_first():
                    qst_post['body']=tbody.css(".question-body").extract_first()
                    #无法正常获取score值与following值，score转到titlelist中获取 #updated:使用splash xvbf 运行js页面
                    #tst=tbody.css(".post-score").extract()
                    #tst=tbody.css(".favorite-count").extract()
                    #qst_post['score']=int(tbody.css(".post-score ::text").extract_first())
                    qst_post['following_count']=0 if not tbody.css(".favorite-count ::text").extract_first() else int(tbody.css(".favorite-count ::text").extract_first())
                    qst_post['score']=item['score']
                    creation_time = datetime_timestamp(tbody.css(".friendly-date ::attr(title)").extract_first(), "%Y, %b %d at %H:%M:%S+0000")  # timestamp now
                    qst_post['creation_time'] = timestamp_datetime(creation_time)  # str now
                    if not qst_post['creation_time']:
                        print "no creation_time"
                    if tbody.css(".staff").extract_first():
                        qst_post["is_staff"]="True"
                    else:
                        qst_post["is_staff"] = "False"
                    # todo:comments
                    break
                else:
                    print "cannot find question"
            if (qst_post['body']=="scrapy failed"):
                print "scrapy failed"

            item['posts'].append(dict(qst_post))

        #2. answer
        for answer in response.css(".answer"):
            asr_post=PostItem()
            asr_post["post_type"] = "answer"
            asr_post["post_id"] = answer.css("::attr(nodeid)").extract_first()
            asr_post["parent_id"] = item["discussion_id"]
            asr_post["body"] = answer.css(".answer-body").extract_first()  #注意，此处不是text而是获取answerbody下所有
            asr_post["score"] = int(answer.css('.post-score ::text').extract_first())
            asr_post["is_staff"] = "False"
            if answer.css(".staff").extract_first():
                asr_post["is_staff"] = "True"

            if answer.css(".accepted-answer").extract_first():
                asr_post["accept_id"] = asr_post["post_id"]
                item['posts'][0]["accept_id"] = asr_post["post_id"]
            else:
                asr_post["accept_id"] ="NULL"

            sql_find_qst_id= "SELECT * FROM tbl_ueanswers_post WHERE Id = %s AND PostType = %s"
            count = self.dbc.cursor.execute(sql_find_qst_id,(asr_post["post_id"], "question"))
            if count>0:
                print "ERROR: answer id conflict with question id!!!!!!!!!!!!!!!!!!"

            creation_time = answer.css('.friendly-date ::attr(title)').extract_first()
            creation_time = datetime_timestamp(creation_time,"%Y, %b %d at %H:%M:%S+0000")  #timestamp now
            asr_post["creation_time"] =timestamp_datetime(creation_time)
            item["posts"].append(dict(asr_post))
            #print item
        yield item

    def wrap_item_4_fix(self, item):
        if int(item['other_info']['last_view_count']) == int(item['view_count']):
            print "view_countstr = "+ str(item['other_info']['last_view_count'])
            return False
        summury_info={
            "Section":item['other_info']['section'],
            "SectionLink":item['other_info']['section_link'],
            "Version":item['other_info']['version'],
            "ViewCount":item['view_count'],
            "Score":item['score'],
            'Imported': "False"
        }
        other_info = {'process_type': "Fix or Update",
                      'summury_info':summury_info
                      }
        item['other_info'] = other_info
        return True

    def wrap_item_4_sumury(self, item):
        if int(item['other_info']['last_view_count']) == int(item['view_count']):
            print "view_countstr = "+ str(item['other_info']['last_view_count'])
            return False
        summury_info={
            #"Section":item['other_info']['section'],
            #"SectionLink":item['other_info']['section_link'],
            #"Version":item['other_info']['version'],
            "ViewCount":item['view_count'],
            "Score":item['score'],
            "Imported":"False"
        }
        other_info = {'process_type': "Fix or Update",
                      'summury_info':summury_info}
        item['other_info'] = other_info
        return True

    def wrap_item_with_summury(self, item):
        if g_fix:
            return self.wrap_item_4_fix(item)
        elif g_update_summury_info:
            return self.wrap_item_4_sumury(item)

    def get_stop_summury_update_time(self):
        foo="todo"





