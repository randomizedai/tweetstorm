'''
Created on Aug 10, 2014

@author: Amit
'''

import subprocess
import smtplib,email,email.encoders,email.mime.text,email.mime.base,ntpath
import numpy as np
import time
from database import *
from util import *
texcommand = "/usr/bin/pdflatex"
import os

def latex_red(string):
    return "\\textcolor{red}{\\textbf{" + string + "}}"

def mysql_status_string(con,debug=False):
    try:
        ans = test_mysql_con(0, con, debug)
        if ans:
            return "MySQL Server - - running"
        else:
            return "MySQL Server - - " + latex_red("not running")
    except Exception,e:
        return "???"

    
def twitter_accounts_string(con,debug=False):
    try:
        ans = select_from_table(con, 0, "twitter_auths", "COUNT(*) as count", {}, debug=debug)
        total_accounts = ans['count']
    #select Id,active_status,last_access,last_status_desc,round((CURRENT_TIMESTAMP - last_access) / 60,2) as minutes from twitter_auth order by minutes;""
        ans2 = select_from_table(con, 0, "twitter_auths", "*,round((CURRENT_TIMESTAMP - last_access) / 60,2) as minutes", bool_dict={},having_dict={"minutes<":30},count="all", debug=debug)
    
        active_accounts = len(ans2)
        active_accounts_string = str(len(ans2)) 
        if active_accounts < total_accounts:
            active_accounts_string = latex_red(active_accounts_string)
    
        return "Total Twitter Accounts - - " + str(total_accounts) + "\\newline" + " Active Twitter Accounts -- " + str(active_accounts_string) 
    except Exception,e:
        return "???"

def total_users_string(con,debug=False):    
    try:
        ans = select_from_table(con, 0, "users", "COUNT(distinct screenname) as count", {}, debug=debug)
        total_users = ans['count']
        return "Total Users - - " + str(total_users)
    except Exception,e:
        return "???"

def total_keywords_string(con,debug=False):    
    try:
        ans = select_from_table(con, 0, "keywords", "COUNT(distinct keyword) as count", {}, debug=debug)
        total_users = ans['count']
        return "Total Keywords - - " + str(total_users)
    except Exception,e:
        return "???"

def total_machines_string(con,starttime,endtime,debug=False):
    try:
        query = "select distinct(f.machine_name) from download_logs as dl  JOIN files as f  on dl.file_id = f.id\
                 where download_time > \'" + starttime + "\' and download_time < \'" + endtime + "\'";
        ans = general_select_query(con, 0, query, count = "all",debug=debug)
        ans_string = "Active Machines - - " + str(len(ans)) + " [" + ",".join([x['machine_name'] for x in ans]) + "]"
        return ans_string
    except Exception,e:
        return "???"


def total_tweets_downloaded(con,starttime,endtime,debug=False):
    try:
        query = "select SUM(count) as sum from download_logs\
                 where download_time > \'" + starttime + "\' and download_time < \'" + endtime + "\'";
        ans = general_select_query(con, 0, query, count ="one",debug=debug)
        
        ans_string = "Total Tweets Downloaded - - " + str(ans['sum'])
        return ans_string
    except Exception,e:
        return "???"



def query_tweets_downloaded(con,starttime,endtime,query_type,debug=False):
    query_type_lower = query_type.lower()
    try:
        query = "select SUM(count) as sum, COUNT(distinct query_id) as count from download_logs WHERE query_type =\'" + query_type_lower + "\'\
                 and download_time > \'" + starttime + "\' and download_time < \'" + endtime + "\'";
        ans = general_select_query(con, 0, query, count ="one",debug=debug)
        ans_string = query_type + " Tweets Downloaded - - " + str(ans['sum']) + " \\newline " + "Number of " + query_type + " used - - " + str (ans['count'])
        return ans_string
    except Exception,e:
        return "???"

def total_tweets_in_db(con,debug=False):
    try:
        query1 = "select SUM(count) as sum from download_logs";        
        ans1 = general_select_query(con, 0, query1, count ="one",debug=debug)
        query2 = "select SUM(size) as sum from files";
        ans2 = general_select_query(con, 0, query2, count ="one",debug=debug)
        ans_string = "Total Tweets in Database - - " + str(ans1['sum']) + " ( " + str(ans2['sum']) + " MB )"
        return ans_string
    except Exception,e:
        return "???"
    
def generate_pdf(filename, clean=True):
    try: 
        command = texcommand + ' --output-directory=' + get_absolute_path("reports/") + " " + \
            filename + '.tex'
        print command    
        os.system(command)

        if clean:
            os.system('rm ' + filename + '.aux ' +
                            filename + '.log')
        
        return filename + ".pdf"    
    except Exception,e:
        pass        

def get_top_users(con,starttime,endtime,debug=False):
    query = "select distinct(u.id) as sn,sum(dl.count) as sum from download_logs as dl, users as u \
      where dl.query_id = u.id and dl.query_type = \"users\" and dl.download_time > \'" + starttime + "\' and dl.download_time < \'" + endtime + "\'\
      group by u.id order by sum desc limit 10"
    ans =  general_select_query(con, 0, query, count ="all",debug=debug)
    s = "\\begin{tabular}{|c|c|c|}\
         \\hline\
         Rowid & Total Tweets \\\\ \n \\hline\n"
    for row in ans:
        s = s + str(row['sn']).replace("_","\\_") + " & " + str(row['sum']) + "\\\\ \n \\hline\n" 
    s = s + "\\end{tabular}"  
    return s           

def get_top_keywords(con,starttime,endtime,debug=False):
    query = "select distinct(u.keyword) as sn,sum(dl.count) as sum from download_logs as dl, keywords as u \
      where dl.query_id = u.id and dl.query_type = \"keywords\" and dl.download_time > \'" + starttime + "\' and dl.download_time < \'" + endtime + "\'\
      group by u.id order by sum desc limit 10"
    ans =  general_select_query(con, 0, query, count ="all",debug=debug)
    s = "\\begin{tabular}{|c|c|}\
         \\hline\
         Keyword & Total Tweets \\\\ \n \\hline\n"
    for row in ans:
        s = s + str(row['sn']).replace("_","\\_") + " & " + str(row['sum']) + "\\\\ \n \\hline\n" 
    s = s + "\\end{tabular}"  
    return s 

def generate_report(config,last_report_generated_time,cur_time,debug=False):
    endtime = get_timestamp_from_time(cur_time,'%Y-%m-%d %H:%M:%S')
    starttime = get_timestamp_from_time(last_report_generated_time,'%Y-%m-%d %H:%M:%S')
    endtime_file = get_timestamp_from_time(cur_time,'%Y%m%d_%H%M%S')
    starttime_file = get_timestamp_from_time(last_report_generated_time,'%Y%m%d_%H%M%S')
    con = get_mysql_con(0, config, debug)
    
    string = "\\title{\\textbf{Tweets Download Report}}\n \
              \\date{}\n  \
              \\maketitle\n \
              \\centerline{\\textbf{Start-time} :- " + starttime + " \\hspace{40pt} \\textbf{End-time} :- " + endtime +"} \
              \\subsection*{Overall Status} \
               \\begin{itemize} \
               \\item " + mysql_status_string(con,debug) + "\
               \\item " + twitter_accounts_string(con,debug) + "\
               \\item " + total_users_string(con,debug) + "\
               \\item " + total_keywords_string(con,debug) + "\
               \\item " + total_machines_string(con,starttime,endtime,debug) + "\
               \\item " + total_tweets_in_db(con,debug) + "\
               \\end{itemize}\
               \\subsection*{Download Stats} \
               \\begin{itemize} \
               \\item " + total_tweets_downloaded(con, starttime, endtime, debug) + "\
               \\item " + query_tweets_downloaded(con, starttime, endtime,"Users", debug) + "\
               \\item " + query_tweets_downloaded(con, starttime, endtime, "Keywords", debug) + "\
              \\end{itemize}\
              \\subsection*{Top Users}" + get_top_users(con, starttime, endtime, debug)\
              +  "\\subsection*{Top Keywords}" + get_top_keywords(con, starttime, endtime, debug)
              
              
              
    
    
    header = "\\documentclass{article}\\usepackage[T1]{fontenc}\n\
              \\usepackage[utf8]{inputenc}\n\
              \\usepackage{lmodern}\n \
              \\usepackage{amsmath}\n \
               \\usepackage{tikz}\n \
               \\usepackage{color}\n \
               \usepackage[margin=0.5in]{geometry}"
    
    document_string = header + "\\begin{document}" + string + "\\end{document}"           
    filename = get_absolute_path("reports/report_" + starttime_file + "__" + endtime_file )
    with open(filename + ".tex","w") as fp:
        fp.write(document_string + "\n")
    return generate_pdf(filename,clean = True)
    
    
    
def send_mail(smtpserver,username,password,from_address,subject,recipients,attachments_list):
  # create html email
  html = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" '
  html +='"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"><html xmlns="http://www.w3.org/1999/xhtml">'
  html +='<body style="font-size:12px;font-family:Verdana"><p>Please find attached !! </p>'
  html += "</body></html>"
  emailMsg = email.MIMEMultipart.MIMEMultipart('alternative')
  emailMsg['Subject'] = subject
  emailMsg['From'] = from_address
  emailMsg['To'] = recipients
  emailMsg.attach(email.mime.text.MIMEText(html,'html'))

  # now attach the files ( { filetype : filepath})
  for filetype,filepath in attachments_list:
      fileMsg = email.mime.base.MIMEBase('application',filetype)
      filev = file(filepath)
      filename = ntpath.basename(filev.name)
      fileMsg.set_payload(filev.read())
      email.encoders.encode_base64(fileMsg)
      fileMsg.add_header('Content-Disposition','attachment;filename=' + filename)
      emailMsg.attach(fileMsg)

  # send email
  server = smtplib.SMTP(smtpserver)
  server.starttls()
  server.login(username,password)
  for x in recipients.split(","):
      server.sendmail(from_address,x.strip(),emailMsg.as_string())
  server.quit()


def generate_and_send_report(config,last_report_generated_time,cur_time,debug=False):
    path = generate_report(config,last_report_generated_time,cur_time,debug)
    if debug:
        print "Waiting for latex compilation"
    time.sleep(30)
    if path:
        if debug:
            print "Sending mail"
        send_mail(config.get('smtp','smtpserver'),config.get('smtp','username'),config.get('smtp','password'),\
                  config.get('email','from_address'),"Tweets Download Report For " + get_current_date(), config.get('email','recipients'),[("pdf",path)])        
    

if __name__ == '__main__':
    config = read_config_file(get_absolute_path("config.ini"))
    generate_report(config, 0, time.time(),True)