# -*- coding: utf-8 -*-

import subprocess

cmd_str="scrapy crawl u3dforum_spider"
cmd_directory="D:\MajorProjects\Projects_UWA\UWA_spiders\spiders\unity3d_answers\unity3d_answers"

def run_shell_command(command):
    p=subprocess.Popen(command,stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin= subprocess.PIPE, shell=True,cwd=cmd_directory)
    for i in iter(p.stdout.readline, b''):
        print i.rstrip()

    #return p.communicate()

run_shell_command(cmd_str)

