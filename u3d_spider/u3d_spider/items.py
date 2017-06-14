# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
'''
class QuestionTag(scrapy.Item):
    """ 问题标签 """
    link = scrapy.Field()   # 标签链接
    name = scrapy.Field()   # 名称
    nodeid = scrapy.Field() # 标签ID


class QuestionListItem(scrapy.Item):
    """ 问题列表Item """
    link = scrapy.Field()        # 问题链接
    nodeid = scrapy.Field()      # 问题ID
    title = scrapy.Field()       # 标题
    update_at = scrapy.Field()   # 更新日期
    tags = scrapy.Field()        # 标签列表


class QuestionItem(scrapy.Item):
    """ 问题内容Item """
    pass
'''

class QuestionItem(scrapy.Item):
    question_id = scrapy.Field()
    link = scrapy.Field()
    title = scrapy.Field()
    state = scrapy.Field()
    update_time = scrapy.Field()
    reply_num = scrapy.Field()
    following_num = scrapy.Field()
    tags = scrapy.Field()  # 包含所有Tag
    contents = scrapy.Field() #包含所有Content


class ContentItem(scrapy.Item):
    '''问题内容，包括回答内容'''
    content_id = scrapy.Field()
    content = scrapy.Field()
    vote = scrapy.Field()

    is_question = scrapy.Field()
    is_answer = scrapy.Field()
    comments = scrapy.Field() #包含所有comments


class TagItem(scrapy.Item):
    tag_id = scrapy.Field()
    link = scrapy.Field()
    tag = scrapy.Field()

class CommentItem(scrapy.Item):
    comment_id = scrapy.Field()
    comment = scrapy.Field()
    content_id = scrapy.Field()



class DiscussionItem(scrapy.Item):
    discussion_id = scrapy.Field()
    link = scrapy.Field()
    title = scrapy.Field()
    creation_time = scrapy.Field()
    update_time = scrapy.Field()
    reply_count = scrapy.Field()
    view_count = scrapy.Field()
    tags = scrapy.Field()  # 包含所有Tag
    posts = scrapy.Field() #包含所有Post
    state = scrapy.Field()

class PostItem(scrapy.Item):
    post_id = scrapy.Field()
    parent_id = scrapy.Field()
    link = scrapy.Field()
    post_type= scrapy.Field()
    title = scrapy.Field()
    creation_time = scrapy.Field()
    update_time = scrapy.Field()
    body = scrapy.Field()
    post_number = scrapy.Field()
    #banner = scrapy.Field()
    is_staff = scrapy.Field()
    score = scrapy.Field()
    following_count = scrapy.Field()
    accept_id=scrapy.Field()
