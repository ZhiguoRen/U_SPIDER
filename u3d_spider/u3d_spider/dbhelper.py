# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()

class QuestionList(Base):
    __tablename__ = 'tb_question'

    question_id = Column(Integer, primary_key=True)
    link = Column(String)
    title = Column(String)
    state = Column(String)
    update_time = Column(String)
    content_id = Column(Integer,ForeignKey('tb_content.content_id'))
    answer_id = Column(Integer,ForeignKey('tb_content.content_id'))
    user = relationship("User", back_populates="addresses")

    def __repr__(self):
        return "<Address(email_address='{}')>".format(self.email_address)
