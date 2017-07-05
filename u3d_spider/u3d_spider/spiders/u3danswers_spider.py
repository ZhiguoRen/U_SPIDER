# -*- coding: utf-8 -*-
import scrapy

from ..db_mysql.DBController import DBController
from ..items import TagItem, PostItem, DiscussionItem

from ..spider_utilities import timestamp_datetime,datetime_timestamp

from scrapy.conf import settings
import time


g_update_state_db = True #记录爬取状态
g_force_crawl = True    #强制继续crwal， if发现未update问题
g_force_update = True  #强制写db
g_stop_time = datetime_timestamp("2016-01-01", "%Y-%m-%d")
g_stop_num_threshhold = 15 #当发现多余threshhold个未更新的问题时，停止crawl(多线程原因)

class U3dAnswersSpider(scrapy.Spider):
    name = "u3danswers_spider"
    allowed_domains = ["answers.unity3d.com"]
    # 如爬取中断，查看log并修改这里的page id重新下载
    start_urls = [ "http://answers.unity3d.com/index.html?page=1160&pageSize=30&sort=active&customPageSize=true&filters=all"]




    def __init__(self, *a, **kw):
        super(U3dAnswersSpider, self).__init__(*a, **kw)
        self.finish_crawl=False
        #self.s_factor=100
        self.dbc = DBController(host=settings['MYSQL_SERVER'],
                                db_user_name=settings['MYSQL_USER'],
                                psd=settings['MYSQL_PSW'],
                                port=settings['MYSQL_PORT'],
                                db_name=settings['MYSQL_DB'])

    def parse(self, response):   #负责生成翻页request，生成question_item request, 记录question_title中的概括信息
        if self.finish_crawl:
            print "u3danswers_spider finish crawl"
            yield

        url = response.url

        # 保存当前页面in db_state
        if g_update_state_db:
            sql_update_state_db = "INSERT INTO tbl_u3danswers_spider_state (ScrapingTime, QuestionId, Link, QuestionUpdateTime, Finish) VALUES (%s, %s, %s, %s, %s)"
            self.dbc.execute_SQL(sql_update_state_db, (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),"NULL",url,"NULL","False"))



        found_questions_num=0

        question_list = response.css('.question-list-item')
        cur_priority = len(question_list) * 20 #+ self.s_factor
        for question in question_list:

            #item = QuestionItem()
            item = DiscussionItem()
            item['link'] = response.urljoin(question.css('.title a::attr(href)').extract_first())
            #item['question_id'] = item['link'].split('/')[-2]
            item['discussion_id']=item['link'].split('/')[-2]
            item['title'] = question.css('.title>a::text').extract_first()
            update_time = question.css('.last-active-user span::attr(title)').extract_first()
            if not update_time:
                print "Exception: update_time is None"
                continue


            #update_time = datetime.strptime(update_at, settings['DATE_FORMAT'])
            update_time = datetime_timestamp(update_time, settings['DATE_FORMAT'])  #timestamp now
            item['update_time'] = timestamp_datetime(update_time)  #str now
            item['posts'] = []
            item['state']='notAccepted'
            item['reply_count'] = question.css('.answers span::text').extract_first()
            if question.css('.accepted').extract_first():
                print "state: accepted"
                item['state'] = 'Accepted'

            if not g_force_crawl:
                #判断日期是否可以结束
                if update_time<=g_stop_time:
                    print "can stop: update_time:"+str(update_time)+"   older then stop_time:"+str(g_stop_time)
                    self.finish_crawl = True
                    break


            # 判断更新日期，若未更新，则pass
            sql_find_update_time="SELECT UpdateTime FROM tbl_u3danswers_post WHERE Id = %s"
            row_count=self.dbc.execute_SQL(sql_find_update_time,item['discussion_id'])
            if row_count > 0:
                last_update_time = self.dbc.cursor.fetchone()[0]
                last_update_time = datetime_timestamp(last_update_time)
                if last_update_time >= update_time:
                    print "question not updated: "+item['discussion_id']
                    found_questions_num = found_questions_num+1
                    print "found question not updated num: "+ str(found_questions_num)
                    if not g_force_crawl and (found_questions_num>g_stop_num_threshhold):
                        print "finish_crawl"
                        self.finish_crawl=True
                        break
                    if not g_force_update:
                        print "skip this question"
                        continue
                print "question updated:" +item['discussion_id'] +"  last_update_time-" + str(last_update_time) + "    update_time-" + str(update_time)
            else:
                print "found question: "+item['discussion_id']

            item['tags'] = []
            for t in question.css('.tags .tag'):
                tag_item = TagItem()
                tag_item['link'] = response.urljoin(t.css('::attr(href)').extract_first())
                tag_item['tag_id'] = t.css('::attr(nodeid)').extract_first()
                tag_item['tag'] = t.css('::text').extract_first()
                item['tags'].append(dict(tag_item))
            #yield item
            #生成request来爬取question内容
            yield scrapy.Request(response.urljoin(item['link']), meta={'item':item}, callback=self.parse_question,priority=cur_priority)
            cur_priority=cur_priority-10

        #生成下一页的request
        if not self.finish_crawl: #不再继续翻页
            next_page = response.css('.pagination .next a::attr(href)').extract_first()
            # if (not self.check_last_update_date or self.last_update_date is None or page_end_date >= self.last_update_date) and (next_page is not None):
            if (next_page is not None):
                print "next page request"
                #self.s_factor=self.s_factor-len(question_list)
                yield scrapy.Request(response.urljoin(next_page), callback=self.parse, priority=-1)
            else:
                print "cannot  next pagefind，current page: "+ response.url


    def parse_question(self,response):
        item = response.meta['item']
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
            question_content = response.css('.question')
            qst_post['body'] = question_content.css('.question-body').extract_first()
            qst_post['score'] = question_content.css('.score ::text').extract_first()
            qst_post['following_count'] = response.css('.question-follow-widget strong::text').extract_first()
            #todo:comments

            item['posts'].append(dict(qst_post))

        #2. answer
        for answer in response.css(".answer-list .answer-container"):
            asr_post=PostItem()
            asr_post["post_type"] = "answer"
            asr_post["post_id"] = answer.css(".answer-gravatar ::attr(nodeid)").extract_first()
            asr_post["parent_id"] = item["discussion_id"]
            asr_post["body"] = answer.css(".answer-body").extract_first()
            asr_post["score"] = answer.css('.score ::text').extract_first()
            if answer.css(".label-success").extract_first():  #Fixed： 之前版本此处判断刚好相反，导致db中该位置为错
                asr_post["accept_id"] = asr_post["post_id"]
                item['posts'][0]["accept_id"] = asr_post["post_id"]
            else:
                asr_post["accept_id"] ="NULL"

            sql_find_qst_id= "SELECT * FROM tbl_u3danswers_post WHERE Id = %s AND PostType = %s"
            count = self.dbc.cursor.execute(sql_find_qst_id,(asr_post["post_id"], "question"))
            if count>0:
                print "ERROR: answer id conflict with question id!!!!!!!!!!!!!!!!!!"

            #无法准确获取时间（存在 hours ago...）
            #creation_time = answer.css(".post-info.muted ::text").extract_first().split(".")[-1]
            #asr_post["creation_time"] =timestamp_datetime(datetime_timestamp(creation_time, settings['DATE_FORMAT']))
            item["posts"].append(dict(asr_post))

        # 下一页，返回request，并且传递item
        next_page=response.css(".pagination .next a::attr(href)").extract_first()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), meta={'item':item}, callback=self.parse_question,priority=1000)
        else:
            #print item
            yield item







