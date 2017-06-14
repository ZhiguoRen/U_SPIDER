# -*- coding: utf-8 -*-
import scrapy
#from unity3d_answers.items import QuestionListItem, QuestionTag
from ..db_mysql.DBController import DBController
from ..items import QuestionItem,ContentItem,CommentItem,TagItem

from scrapy.conf import settings
from datetime import datetime

class QuestionListSpider(scrapy.Spider):
    name = "question-list"
    allowed_domains = ["answers.unity3d.com"]
    # 如爬取中断，查看log并修改这里的page id重新下载
    start_urls = ['http://answers.unity3d.com/?page=1&pageSize=100&sort=active&customPageSize=true&filters=all/']
    last_update_date = None

    def __init__(self, *a, **kw):
        super(QuestionListSpider, self).__init__(*a, **kw)
        self.dbc = DBController(host=settings['MYSQL_SERVER'],
                                db_user_name=settings['MYSQL_USER'],
                                psd=settings['MYSQL_PSW'],
                                port=settings['MYSQL_PORT'],
                                db_name=settings['MYSQL_DB'])




    #def __init__(self, check_last_update_date=True):
        # If False, crawl all "next pages" without comparing update dates
        self.check_last_update_date=True
        if 'check_last_update_date' in kw.keys():
            if kw['check_last_update_date'] == 'False':
                check_last_update_date = False

                self.check_last_update_date = check_last_update_date
        #for debug
        self.last_update_date = datetime.strptime(settings['START_DATE'], settings['DATE_FORMAT'])

    def set_last_update_date(self, last_update_date):
        self.last_update_date = last_update_date






    def parse(self, response):
        question_list = response.css('.question-list-item')
        for question in question_list:
            item = QuestionItem()
            item['link'] = question.css('.title a::attr(href)').extract_first()
            item['question_id'] = item['link'].split('/')[-2]
            item['title'] = question.css('.title>a::text').extract_first()
            update_at = question.css('.last-active-user span::attr(title)').extract_first()
            if not update_at:
                continue
            update_time = datetime.strptime(update_at, settings['DATE_FORMAT'])
            item['update_time'] = update_at
            item['contents'] = []

            item['state']='notAccepted'
            item['reply_num'] = question.css('.answers span::text').extract_first()
            if question.css('.accepted').extract_first():
                print "state: accepted"
                item['state'] = 'Accepted'


            # 强制更新数据库 todo:修改位置
            forceUpdate = False

            #以update日期排序，如果该页中存在not updated的问题，则之后页的所有问题都未updated
            skipNextPage = False



            # 判断更新日期，若未更新，则pass
            sql_find_update_time="SELECT update_time FROM tbl_question WHERE question_id = %s"
            row_count=self.dbc.cursor.execute(sql_find_update_time,item['question_id'])
            if row_count > 0:
                last_update_time = self.dbc.cursor.fetchone()
                last_update_time = datetime.strptime(last_update_time[0],settings['DATE_FORMAT'])
                if last_update_time >= update_time:
                    print "question not updated: "+item['question_id']
                    if not forceUpdate:
                        skipNextPage = True
                        print "skip"
                        continue
                print "question updated:" +item['question_id'] +"  last_update_time-" + str(last_update_time) + "    update_time-" + str(update_time)
            else:
                print "found question: "+item['question_id']

            item['tags'] = []
            for t in question.css('.tags .tag'):
                tag_item = TagItem()
                tag_item['link'] = t.css('::attr(href)').extract_first()
                tag_item['tag_id'] = t.css('::attr(nodeid)').extract_first()
                tag_item['tag'] = t.css('::text').extract_first()
                item['tags'].append(dict(tag_item))
            #yield item
            #生成request来爬取question内容
            yield scrapy.Request(response.urljoin(item['link']), meta={'item':item}, callback=self.parse_question)

        # 比较当前页最后一个问题的update日期和上次更新日期
        #page_end_date = question_list[-1].css('.last-active-user span::attr(title)').extract_first()
        #page_end_date = datetime.strptime(page_end_date, settings['DATE_FORMAT'])

        next_page = response.css('.pagination .next a::attr(href)').extract_first()
        #if (not self.check_last_update_date or self.last_update_date is None or page_end_date >= self.last_update_date) and (next_page is not None):
        if (not skipNextPage) and (next_page is not None):
            print "next page request"
            yield scrapy.Request(response.urljoin(next_page), callback=self.parse)
        else:
            print "skip next page"

    def parse_question(self,response):
        item = response.meta['item']

        item['following_num'] = response.css('.question-follow-widget strong::text').extract_first()
        print item['following_num']

        question_content=response.css('.question')




        qst_content_item = ContentItem()
        qst_content_item['content_id'] = item['question_id']
        qst_content_item['is_question'] = 'True'
        qst_content_item['content'] = question_content.css('.question-body').extract_first()
        qst_content_item['is_answer'] = 'False'
            #todo:comments
        qst_content_item['vote'] = question_content.css('.score ::text').extract_first()



        item['contents'].append(dict(qst_content_item))
        for content in response.css(".answer-list .answer-container"):
            content_item=ContentItem()
            content_item['content_id'] = content.css(".answer-gravatar ::attr(nodeid)").extract_first()
            content_item['is_question'] = 'False'
            content_item['content'] = content.css(".answer-body").extract_first()
            success_lable = content.css(".label-success").extract_first()
            if not success_lable:
                content_item['is_answer'] = "False"
            else:
                content_item['is_answer'] = "True"
            content_item['vote'] = content.css('.score ::text').extract_first()
            # todo: comments
            item['contents'].append(dict(content_item))

        # 下一页，返回request，并且传递item
        next_page=response.css(".pagination .next a::attr(href)").extract_first()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), meta={'item':item}, callback=self.parse_question)
        else:
            #print item
            yield item







