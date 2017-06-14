#!/usr/bin/python
# -*- coding: UTF-8 -*-
# @Author  : ren@uwa4d.com

import pymysql
import hashlib
import time

class DBController:
    def __init__(self, host, port, db_name, db_user_name, psd,reconnect=False):
        self.port = port
        self.host = host
        self.db_name = db_name
        self.db_user_name = db_user_name
        self.psd = psd
        #self.table_proj_name = table_proj_name
        #self.catch_table_proj_name = catch_table_proj_name
        self.db_wait_timeout=14400 #seconds
        self.db_last_qtime=time.time()
        self.reconnect=reconnect
        self._connection()

    def _check_timeout(self):
        #print "_check_timeout"
        #print time.time()
        #print self.db_last_qtime
        #print self.db_wait_timeout
      #  print ((time.time()- self.db_last_qtime)-self.db_wait_timeout)
        if self.reconnect:
            print "reconnect"
            self.stop()
            self._connection()
            self.db_last_qtime = time.time()
        else:
            if time.time()- self.db_last_qtime >= self.db_wait_timeout:
                print 'db need reconnect'
                self.stop()
                self._connection()
                self.db_last_qtime=time.time()

    def _connection(self):
        print "connecting"
        try:
            self.conn = pymysql.connect(host=self.host, user=self.db_user_name, passwd=self.psd, port=self.port, charset='utf8')
            self.cursor=self.conn.cursor()
            self.cursor.execute("USE " + self.db_name)
        except Exception,e:
            print e


    def execute_SQL(self, sql_str, args=None):
        try:
            self._check_timeout()
            ret=self.cursor.execute(sql_str,args)
            self.conn.commit()
        except Exception, e:
            print e
            self.try_to_reconnect()
            ret = self.cursor.execute(sql_str,args)
            self.conn.commit()
        #print ret
        return ret

    def get_int_time(self, offset=-1000):
        return int(time.strftime("%Y%m%d%H%M%S", time.localtime())) + offset

    def stop(self):
        print "stop"
        self.cursor.close()
        print "cursor.closed"
        # self.conn.commit()
        # print "conn.commited"
        self.conn.close()
        print "conn.closed"

    def try_to_reconnect(self):
        self.stop()
        self._connection()


    def get_md5hash(self,str):
        return hashlib.md5(str).hexdigest().encode('utf-8').strip()


if __name__ == '__main__':
    db_controller = DBController(host='localhost', db_user_name='root', psd='',
                                   port=3306, db_name='uwa_spider')
    db_controller.execute_SQL("select * from information_schema.tables")
    print db_controller.cursor.fetchall()
    #print pid
    # row,info=db_controller.fetch_project_info_all()
    # print info
    # state=db_controller.fetch_state_by_pid(pid)
    # print state
    # db_controller.update_project(pid,"STOP_SUCCESS",db_controller.get_int_time())
    # state = db_controller.fetch_state_by_pid(pid)
    # print state
    # db_controller.delete_project(pid)
    # db_controller.fetch_project_info_all()