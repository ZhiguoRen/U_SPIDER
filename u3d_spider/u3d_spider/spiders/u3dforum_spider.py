# -*- coding: utf-8 -*-
import scrapy

import time

#from unity3d_answers.items import QuestionListItem, QuestionTag
from ..db_mysql.DBController import DBController
from ..items import DiscussionItem,PostItem,TagItem


from ..spider_utilities import timestamp_datetime, datetime_timestamp, datetime_timestamp_sticky
from scrapy.conf import settings
#from datetime import datetime

from scrapy.http import Request

import sys

g_force_update=False              #是否强制更新数据库
g_force_crawl=True               #是否强制crwal
g_update_state_db=True           #更新状态数据库
g_start_by_state_db=False        #从数据库中页面开始


#g_finish_crwal=False             #当： g_force_crwal=False&&论坛已经crwal过一遍&&从首页开始crwal&&发现未update的条目时置为True

g_start_url_index = 0

#tbl_state中page - 2页开始
g_start_url_list = [
    "https://forum.unity3d.com/forums/android.30/page-134",
    "https://forum.unity3d.com/forums/windows.50/",
"https://forum.unity3d.com/forums/ios-and-tvos.27/",
    "https://forum.unity3d.com/forums/unity-ui-textmesh-pro.60/",
    "https://forum.unity3d.com/forums/extensions-ongui.25/",
    "https://forum.unity3d.com/forums/virtual-reality.80/"

]

'''
f=open(r'd:/spider_log.txt','w')
sys.stdout=f
'''
'''
g_start_url_list = ["https://forum.unity3d.com/forums/2d.53/",
                    "https://forum.unity3d.com/forums/audio-video.74/",
                    "https://forum.unity3d.com/forums/animation.52/",
                    "https://forum.unity3d.com/forums/documentation.59/",
"https://forum.unity3d.com/forums/external-tools.22/",
"https://forum.unity3d.com/forums/general-graphics.76/",
                    "https://forum.unity3d.com/forums/editor-general-support.10/page-1030",
                    "https://forum.unity3d.com/forums/scripting.12/page-270",

                    "https://forum.unity3d.com/forums/shaders.16/",


                    "https://forum.unity3d.com/forums/global-illumination.85/",
                    "https://forum.unity3d.com/forums/image-effects.96/",
                    "https://forum.unity3d.com/forums/multiplayer-networking.26/",


                    "https://forum.unity3d.com/forums/physics.78/",
"https://forum.unity3d.com/forums/shaders.16/",
    "https://forum.unity3d.com/forums/global-illumination.85/",
    "https://forum.unity3d.com/forums/image-effects.96/",
        "https://forum.unity3d.com/forums/multiplayer-networking.26/",
    "https://forum.unity3d.com/forums/graphics-experimental-previews.110/",
    "https://forum.unity3d.com/forums/experimental-scripting-previews.107/",
    "https://forum.unity3d.com/forums/particle-system-previews.129/",
    "https://forum.unity3d.com/forums/physics-previews.123/",
    "https://forum.unity3d.com/forums/ai-navigation-previews.122/",
    "https://forum.unity3d.com/forums/timeline-cinemachine.127/",
    "https://forum.unity3d.com/forums/new-input-system.103/",
    "https://forum.unity3d.com/forums/asset-bundles.118/"



                    "https://forum.unity3d.com/forums/general-discussion.14/",
                    "https://forum.unity3d.com/forums/made-with-unity.11/",
                    "https://forum.unity3d.com/forums/works-in-progress.34/",
                    "https://forum.unity3d.com/forums/assets-and-asset-store.32/",
                    "https://forum.unity3d.com/forums/game-design.71/",
                    "https://forum.unity3d.com/forums/meta-forum-discussion.111/",
                    "https://forum.unity3d.com/forums/announcements.9/",
                    "https://forum.unity3d.com/forums/getting-started.82/",
                    "https://forum.unity3d.com/forums/community-learning-teaching.23/",
                    "https://forum.unity3d.com/forums/unity-certification.100/",
                                     "https://forum.unity3d.com/forums/ai-navigation-previews.122/",
                    "https://forum.unity3d.com/forums/timeline-cinemachine.127/",
                    "https://forum.unity3d.com/forums/new-input-system.103/",
                    "https://forum.unity3d.com/forums/asset-bundles.118/",
                    "https://forum.unity3d.com/forums/editor-workflows.117/",
                    "https://forum.unity3d.com/forums/2d-experimental-preview.104/"]
'''


class U3dForumSpider(scrapy.Spider):
    name = "u3dforum_spider"
    allowed_domains = ["forum.unity3d.com"]

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Accept-Language": "en,zh-CN;q=0.8,zh;q=0.6,zh-TW;q=0.4",
        "Cache-Control": "max-age = 0",
        "Connection": "keep-alive",
        "Host":"forum.unity3d.com",
        "Upgrade-Insecure-Requests": "1",
        "Content-Type": " application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }
    #need to change at least once a day....... per session?
    cookies = {
       "SERVERID":"varnish02",
        "_ga":"GA1.3.480431100.1497105557",
        "_gid":"GA1.3.1964130145.1497105557",
        "xf_gan":"S3luB9PbHCyZQv1TYFBg5okLxpxDvKkhGcXrAL6q",
        "xf_session":"d188de182f823ad93c8f18862705ae0c"
        }

    def __init__(self, *a, **kw):
        super(U3dForumSpider, self).__init__(*a, **kw)
        self.start_url_index = g_start_url_index
        #self.finish_crawl = g_finish_crwal
        self.finish_crawl = {"base_tag":False}
        self.dbc = DBController(host=settings['MYSQL_SERVER'],
                                db_user_name=settings['MYSQL_USER'],
                                psd=settings['MYSQL_PSW'],
                                port=settings['MYSQL_PORT'],
                                db_name=settings['MYSQL_DB'])

    def start_requests(self):
        if g_start_by_state_db:
            todo="start by state db"
        else:
            '''
            Request_list=[]
            for url in g_start_url_list:
                Request_list.append(Request(url,cookies = self.cookies,headers=self.headers))
            '''
            return [Request(g_start_url_list[self.start_url_index],cookies = self.cookies,headers=self.headers)]


    def request_next_start_url(self,base_tag):
        self.dbc.cursor.execute("UPDATE tbl_u3dforum_spider_state SET FinishOnce = %s WHERE BaseTag = %s", ("True",base_tag))
        self.dbc.conn.commit()
        self.start_url_index = self.start_url_index + 1
        if self.start_url_index < len(g_start_url_list):
            print "request next start URL:" + g_start_url_list[self.start_url_index]
            return Request(g_start_url_list[self.start_url_index], cookies=self.cookies, headers=self.headers , priority=-1)

        else:
            print "finished; start_url_index: " + str(self.start_url_index)
            self.finish_crawl[base_tag] = True

    def parse(self, response):  #负责生成翻页request，生成item request, 记录title中的概括信息


        url = response.url
        base_tag = url.split('/')[-2].split('.')[-2]
        if base_tag not in self.finish_crawl.keys():
            self.finish_crawl[base_tag]=False

        if self.finish_crawl[base_tag]:
            print "u3dforum_spider finish crwal in base_tag: " + base_tag
            #return

            # 保存当前页面in db_state
        if g_update_state_db:
            sql_update_state_db = "INSERT INTO tbl_u3dforum_spider_state (BaseTag, LastPageLink, LastDiscussionId, LastScrapingTime, FinishOnce) VALUES (%s, %s, %s, %s, %s)" \
                                  "ON DUPLICATE KEY UPDATE LastPageLink=VALUES(LastPageLink), LastScrapingTime=VALUES(LastScrapingTime)"
            self.dbc.cursor.execute(sql_update_state_db, (
            base_tag, response.url, "NULL", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), "False"))
            self.dbc.conn.commit()




        #生成条目的request
        discussion_list = response.css('.discussionListItem')

        #使用priority控制顺序
        cur_priority = len(discussion_list)

        #skipNextPage = False
        for discussion in discussion_list:
            item = DiscussionItem()
            item['link'] = response.urljoin(discussion.css('.title a::attr(href)').extract_first())
            item['discussion_id'] = discussion.css('.discussionListItem ::attr(id)').extract_first().split('-')[-1] #item['link'].split('/')[-2]
            item['title'] = discussion.css('.title>a::text').extract_first()
            creation_time = discussion.css('.startDate .DateTime::attr(data-time)').extract_first()
            update_time = discussion.css('.dateTime .DateTime::attr(data-time)').extract_first()
            is_sticky=False
            if discussion.css('.sticky'):
                is_sticky=True
            # 2 different types of date time
            if not update_time:
                update_time = discussion.css('.dateTime .DateTime::text').extract_first()
                if not update_time:
                    print "Exception: update_time is None"
                    continue
                update_time = datetime_timestamp_sticky(update_time + ' 0:0:0')

            if not creation_time:
                creation_time = discussion.css('.startDate .DateTime::text').extract_first()
                if not creation_time:
                    print "Exception: creation_time is None"
                    continue
                creation_time = datetime_timestamp_sticky(creation_time + ' 0:0:0')

            item['update_time'] = timestamp_datetime(float(update_time))
            item['creation_time'] = timestamp_datetime(float(creation_time))

            item['reply_count'] = filter(unicode.isdigit, discussion.css('.stats .major dd::text').extract_first())
            item['view_count'] = filter(unicode.isdigit, discussion.css('.stats .minor dd::text').extract_first())


            #在crwal一遍后，以update日期排序，如果该页中存在not updated的问题，则之后页的所有问题都未updated
            # 判断更新日期，若未更新，则pass
            sql_find_update_time="SELECT UpdateTime FROM tbl_u3dforum_discussion WHERE Id = %s"
            row_count=self.dbc.cursor.execute(sql_find_update_time,item['discussion_id'])
            if row_count > 0:
                last_update_time = self.dbc.cursor.fetchone()[0]
                #last_update_time = datetime.strptime(last_update_time[0],settings['DATE_FORMAT'])
                last_update_time = datetime_timestamp(last_update_time)

                if last_update_time >= int(update_time):
                    print "discussion not updated: "+item['discussion_id']
                    if (not is_sticky) and (not g_force_crawl):
                        #yield self.request_next_start_url(base_tag)
                        self.finish_crawl[base_tag]=True
                        print "switch 2 next url"
                        break
                    if (not g_force_update):
                        print "skip this discussion"
                        continue
                print "discussion updated:" +item['discussion_id'] +"  last_update_time-" + str(last_update_time) + "    update_time-" + str(update_time)
            else:
                print "found discussion: "+item['discussion_id']

            base_tag_item = TagItem()
            base_tag_item['tag_id'] = base_tag
            base_tag_item['link'] = url
            base_tag_item['tag'] = base_tag
            item['tags'] = [dict(base_tag_item)]
            #item['tags'][0]为base_tag

            tag_list = discussion.css('.tagList li')
            if tag_list:
                for t in tag_list:
                    tag_item = TagItem()
                    tag_href=t.css('a::attr(href)').extract_first()
                    tag_item['link']=response.urljoin(tag_href)
                    tag_item['tag_id']=tag_href.split('/')[-2]
                    tag_item['tag']=t.css('a::text').extract_first()
                    item['tags'].append(dict(tag_item))

            item['posts']=[]
            #生成request来爬取question内容
            yield scrapy.Request(item['link'], meta={'item':item}, callback=self.parse_discussion,cookies = self.cookies,headers=self.headers, priority=cur_priority)
            cur_priority=cur_priority-1

        # 因为request队列后进先出，因此此处应先放置index request
        # 已在配置中修改为先进先出队列
        # 使用priority控制顺序
        if not self.finish_crawl[base_tag]:
            nav_list = response.css('.pageNavLinkGroup.afterDiscussionListHandle .PageNav nav>.text')
            next_page = None
            for navnum in nav_list:
                if navnum.css('a::text').extract_first() == 'Next >':
                    next_page = navnum.css('a::attr(href)').extract_first()

            if (next_page is not None):
                print "generate next page request: " + response.urljoin(next_page)
                yield scrapy.Request(response.urljoin(next_page), callback=self.parse, cookies=self.cookies,
                                     headers=self.headers, priority=-1)
            else:
                yield self.request_next_start_url(base_tag)
                print "cannot find next page, current page:" + response.url
        else:
            # self.finish_crawl=False
            yield self.request_next_start_url(base_tag)


    def parse_discussion(self, response):
        discussion_item = response.meta['item']

        post_list = response.css('.messageList .message')

        for post in post_list:
            post_item = PostItem()
            post_item['parent_id'] = discussion_item['discussion_id']
            post_item['post_id'] = post.css('.message ::attr(id)').extract_first().split('-')[-1]
            post_item['body'] = post.css('.messageText').extract_first()
            post_item['post_number']=post.css('.postNumber ::text').extract_first().split('#')[-1]
            creation_time = post.css('.DateTime ::attr(data-time)').extract_first()
            if not creation_time:
                creation_time = post.css('.DateTime ::text').extract_first()
                creation_time = datetime_timestamp_sticky(creation_time+' 0:0:0')
            creation_time=timestamp_datetime(float(creation_time))
            post_item['creation_time'] = creation_time
            post_item['update_time'] = creation_time
            if post.css('.staff').extract_first():
                post_item['is_staff']='True'
            else:
                post_item['is_staff'] = 'False'
            discussion_item['posts'].append(dict(post_item))

        nav_list = response.css('.PageNav')
        next_page = None
        if nav_list:
            nav_list=nav_list[0].css('nav>.text')

            for navnum in nav_list:
                if navnum.css('a::text').extract_first() == 'Next >':
                    next_page = navnum.css('a::attr(href)').extract_first()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), meta={'item': discussion_item}, callback=self.parse_discussion, priority=100)
        else:
            yield discussion_item







