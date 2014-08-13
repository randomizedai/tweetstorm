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
texcommand = "//usr//texbin//pdflatex"

def generate_pdf(filename, clean=True):

        command = texcommand + ' --jobname="' + filename +  '" "' +' --output-directory=' + get_absolute_path("reports/") + " " + \
            filename + '.tex"'

        subprocess.check_call(command, shell=True)

        if clean:
            subprocess.call('rm "' + filename + '.aux" "' +
                            filename + '.log" "' +
                            filename + '.tex"', shell=True)

def generate_report(config,last_report_generated_time,cur_time,debug=False):
    endtime = get_timestamp_from_time(cur_time,'%Y-%m-%d %H:%M:%S')
    starttime = get_timestamp_from_time(last_report_generated_time,'%Y-%m-%d %H:%M:%S')
    endtime_file = get_timestamp_from_time(cur_time,'%Y%m%d_%H%M%S')
    starttime_file = get_timestamp_from_time(last_report_generated_time,'%Y%m%d_%H%M%S')
    
    string = "\\textcolor{red}{Amit}"
    header = "\\documentclass{article}\\usepackage[T1]{fontenc}\n \
              \\usepackage[utf8]{inputenc} \
              \\usepackage{lmodern} \
              \\usepackage{amsmath} \
               \\usepackage{tikz} \
               \\usepackage{color}"
    
    document_string = header + "\\begin{document}" + string + "\\end{document}"           
    filename = get_absolute_path("reports/report_" + starttime_file + "__" + endtime_file )
    with open(filename + ".tex","w") as fp:
        fp.write(document_string + "\n")
    generate_pdf(filename,clean = True)
    
    
    
def send_mail(smtpserver,username,password,from_address,subject,recipients,attachments_list):
  # create html email
  html = '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" '
  html +='"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"><html xmlns="http://www.w3.org/1999/xhtml">'
  html +='<body style="font-size:12px;font-family:Verdana"><p>...</p>'
  html += "</body></html>"
  emailMsg = email.MIMEMultipart.MIMEMultipart('alternative')
  emailMsg['Subject'] = subject
  emailMsg['From'] = from_address
  emailMsg['To'] = ', '.join(recipients)
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
  server.sendmail(from_address,recipients,emailMsg.as_string())
  server.quit()


def generate_and_send_report(config,last_report_generated_time,cur_time,debug=False):
    path = generate_report(config,last_report_generated_time,cur_time,debug)
            
    

if __name__ == '__main__':
    config = read_config_file(get_absolute_path("config.ini"))
    generate_report(config, 0, time.time(),True)