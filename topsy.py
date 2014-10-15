

##http://otter.topsy.com/search.js?callback=jQuery18307539595430716872_1411572749166&q=climate+change&type=tweet&offset=0&perpage=200
##&mintime=1407340839&maxtime=1407427248&call_timestamp=1411572750193&apikey=09C43A9B270A470B8EB8F2946A9369F3&_=1411572751519



import time
import json
import sys
import requests
import timeout_decorator
import datetime

s = "http://otter.topsy.com/search.js?callback=jQuery1830654582932125777_1411571964536&q=climate+change&type=tweet&offset=0&perpage=100&"
apikey="09C43A9B270A470B8EB8F2946A9369F3"

@timeout_decorator.timeout(30)
def query_topsy(query):
	r = requests.get(query)
	return r
if __name__ == '__main__':
	print sys.path
    
	start_mintime = 1396565824
	ts = int(time.time()*1000)
	ts2 = ts + 1300 
	timegap = 1800
	count = 0
	prefix = "_8"
	f1 = open("output_1sept2012_1hour" + prefix + ".txt","a")
	f2 = open("tweets_climate_change_1sep2012_1hour" + prefix +".txt","a")
	strttime = time.time()
	total = 0
	while (count < 20000):
		query = s + "mintime=" + str(start_mintime) + "&maxtime=" + str(start_mintime + timegap) + "&sort_method=-date&call_timestamp=" + str(ts) + "&apikey=09C43A9B270A470B8EB8F2946A9369F3&_=" + str(ts2)
		try:
			r = query_topsy(query)
		except timeout_decorator.TimeoutError as e:
			print "Timed Out"
			print "waiting for 30 seconds"	
			time.sleep(10)
			continue
	#	print ("abc2")
		
		f1.write(r.text + "\n")
		f1.flush()
		#print ("abc3")
		
		a = r.text
		c = a.find("{")
		j = a[c:-2]
		js = json.loads(j)
		#print ("abc4")
		
		lent = len(js['response']['list'])
		
		for x in js['response']['list']:
			f2.write(json.dumps(x) + "\n" )
		#print ("abc5")
		
		f2.flush()	
		#print ("abc6")
		
		total = total + lent
		print "Start Time  --> " + str(strttime) + " Count --> " + str(count) + "  Tweets --> " + str(lent) + "  Total --> " + str(total) +  "   Cur Time --> " + str(datetime.datetime.fromtimestamp(x['trackback_date']).strftime('%Y-%m-%d %H:%M:%S')) 
		count = count + 1	
		#print ("abc7")
		
		start_mintime = start_mintime + timegap		
	
	print time.time()