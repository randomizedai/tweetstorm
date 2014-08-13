'''
Created on Aug 8, 2014

@author: Amit

Initialize the Database
'''


import MySQLdb as mdb
from database import *
from util import *

config = read_config_file(get_absolute_path("config.ini"))
con = get_mysql_con(0, config, debug=False)
schema = read_config_file(get_absolute_path("schema.ini"))

tables = schema.sections()
for table_name in tables:
    fields_dict = config_section_map(schema,table_name)
    create_table(con, table_name, fields_dict,debug=True)
    
    

