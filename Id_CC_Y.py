import ujson as json
import time
import urllib
import urllib2
import os
import gevent
url = 'http://oxfordhk.azure-api.net/academic/v1.0/evaluate'
expr_max_length = 1750
para = dict()
para['attributes'] = 'Y,AA.AuId,CC'
para['subscription-key'] = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
para['count'] = '50000'
para['offset']='0'
host = 'oxfordhk.azure-api.net'
port = 80
domain = 'academic/v1.0/evaluate'


class Paper_Entity(object):

	def __init__(self, entity):
		self.entity = entity
		self.init_PE()

	def init_PE(self):
		self.CC = self.entity.get('CC', 0)
		self.Y = self.entity.get('Y', 2016)
		self.get_Author_l()

	def get_Author_l(self):
		self.Author_l = []
		for Au in self.entity.get('AA',{}):
			self.Author_l.append(Author_Entity(Au))


class Author_Entity(object):
	
	def __init__(self, entity):
		self.entity=entity
		self.AuId = self.entity.get('AuId',0)
	
	def __str__(self):
		return "%s"%(self.AuId)


import httplib
import urllib
def get_ans(para):
	ans = 0
	src = urllib.urlencode(para)
	httpClient = httplib.HTTPConnection('oxfordhk.azure-api.net', 80)
	httpClient.request('GET', '/academic/v1.0/evaluate?%s'%(src,))
	response = httpClient.getresponse()
	ans = response.read()
	httpClient.close()
	return ans


#@profile
def get_entity(expr):
	
	entities = []
	pe_l = []
	para = dict()
	para['attributes'] = 'AA.AuId,Y,CC'
	para['subscription-key'] = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
	para['count'] = '1000000'
	para['expr'] = expr
	ans = get_ans(para)
	entities = json.loads(ans)['entities']

	for en in entities:
		pe = Paper_Entity(en)
		pe_l.append(pe)
	return pe_l

def Or_expr_AuId(AuId_l):
	length = len(AuId_l)
	ans = 'Composite(AA.AuId=%s)'%(AuId_l[0],)
	for i in xrange(1, length):
		ans = 'Or(%s,Composite(AA.AuId=%s))'%(ans,AuId_l[i])
	return ans

if __name__ =='__main__':
	thread_l = []
	gevent_num = 2
	s = time.time()
	with open('all_author.txt','r') as inf:
		with open('AuId_CC_Y','w') as outf:
			AuId_l = []
			thread_l = []
			AuId_l_l = []
			s = time.time()
			i = 1
			for line in inf.readlines():
				AuId = line.strip()
				i += 1
				if AuId:
					AuId_l.append(AuId)
				if len(AuId_l) >= 2:
					expr = Or_expr_AuId(AuId_l)
					gt = gevent.spawn(get_entity, expr) 
					thread_l.append(gt)
					AuId_l_l.extend(AuId_l)
					AuId_l = []
					if len(thread_l) >= gevent_num:
						gevent.joinall(thread_l)
						AuId_CC = {}
						AuId_Y = {}
						for t in thread_l:
							paper_l = t.value
							for paper in paper_l:
								for Au in paper.Author_l:
									AuId_CC[AuId] = AuId_CC.get(AuId,0) + int(paper.CC)
									AuId_Y[AuId] = min(AuId_Y.get(AuId,2016), int(paper.Y))
						for AuId in AuId_l_l:
							outf.write("%s\t%s\t%s\n"%(AuId, AuId_CC.get(AuId,0), AuId_Y.get(AuId,2016)))
						thread_l = []
						AuId_l_l = []
						print "average time",(time.time() - s) / i
			if len(AuId_l):
				expr = Or_expr_AuId(AuId_l)
				gt = gevent.spawn(get_entity, expr) 
				thread_l.append(gt)
				AuId_l_l.extend(AuId_l)
				AuId_l = []
				if len(thread_l):
					gevent.joinall(thread_l)
					AuId_CC = {}
					AuId_Y = {}
					for t in thread_l:
						paper_l = t.value
						for paper in paper_l:
							for Au in paper.Author_l:
								AuId = str(Au.AuId)
								AuId_CC[AuId] = AuId_CC.get(AuId,0) + int(paper.CC)
								AuId_Y[AuId] = min(AuId_Y.get(AuId,2016), int(paper.Y))
					for AuId in AuId_l_l:
						outf.write("%s\t%s\t%s\n"%(AuId, AuId_CC.get(AuId,0), AuId_Y.get(AuId,2016)))
					thread_l = []
					AuId_l_l = []
					print "average time",(time.time() - s) / i


