import ujson as json
import time
import urllib
import urllib2
import os
import gevent
url = 'http://oxfordhk.azure-api.net/academic/v1.0/evaluate'
expr_max_length = 1750
para = dict()
para['attributes'] = 'Id,AA.AuId,AA.AuN,AA.AfId,AA.AfN,J.JId,J.JN,C.CId,C.CN,F.FId,F.FN,CC,Y,E'
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
		self.Id = self.entity.get('Id')
		self.CC = self.entity.get('CC')
		self.Y = self.entity.get('Y')
		self.Ti = json.loads(self.entity.get('E')).get('DN')
		self.get_Author_l()
		self.get_Field_l()

	def get_Author_l(self):
		self.Author_l = []
		for Au in self.entity.get('AA',{}):
			self.Author_l.append(Author_Entity(Au))

	def get_Field_l(self):
		self.Field_l = []
		for F in self.entity.get('F',{}):
			self.Field_l.append(Field_Entity(F))

	def get_Journary(self):
		self.JId = None
		self.JN = None
		try:
			self.JId = self.entity.get('J').get('JId')
			self.JN = self.entity.get('J').get('JN')
		except:
			print 'no j'


	def get_Conference(self):
		self.CId = None
		self.CN = None
		try:
			self.CId = self.entity.get('C').get('CId')
			self.CN = self.entity.get('C').get('CN')
		except:
			print 'no c'

	def __str__(self):
		FId_str_l = []
		for F in self.Field_l:
			FId_str_l.append(str(F))
		FId_str = '\t'.join(FId_str_l)
		
		Au_Af_str_l = []
		for Au in self.Author_l:
			Au_Af_str_l.append(str(Au))
		Au_Af_str = '\t'.join(Au_Af_str_l)

		return "%s\n%s\n%s\n%s\n%s\n\n" %(self.Id, self.CC, self.Y, FId_str, Au_Af_str)


class Author_Entity(object):
	
	def __init__(self, entity):
		self.entity=entity
		self.AuId = self.entity.get('AuId')
		self.AfId = self.entity.get('AfId')
		self.AuN = self.entity.get('AuN')
		self.AfN = self.entity.get('AfN')
	
	def __str__(self):
		return "%s\t%s"%(self.AuId,self.AfId)

class Field_Entity(object):

	def __init__(self, entity):
		self.entity = entity
		self.FId = entity.get('FId')
		self.FN = entity.get('FN')

	def __str__(self):
		return str(self.FId)




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
def get_entity(expr,count,offset):
	
	entities = []
	pe_l = []
	para = dict()
	para['attributes'] = 'Id,AA.AuId,AA.AuN,AA.AfId,AA.AfN,J.JId,J.JN,C.CId,C.CN,F.FId,F.FN,CC,Y,E'
	para['subscription-key'] = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
	para['count'] = count
	para['offset']= offset
	para['expr'] = expr
	ans = get_ans(para)
	entities = json.loads(ans)['entities']

	for en in entities:
		pe = Paper_Entity(en)
		pe_l.append(pe)
	return pe_l


if __name__ =='__main__':
	thread_l = []
	total = 1*1000
	each_time = 100
	gevent_num = 5
	s = time.time() 
	with open('academic_novae','w') as out:
		with open('academic_novae_Json_dump','w') as out_json:
			print total/each_time
			for i in range(total/each_time):
				print i
				offset = i*each_time
				gt = gevent.spawn(get_entity, "Composite(F.FN='computer science')", each_time, offset) 
				thread_l.append(gt)
				if len(thread_l)>=gevent_num:
					print 'gevent start to get: ',i*each_time
					gevent.joinall(thread_l)
					print "average:",(time.time()-s)/i/each_time
					for t in thread_l:
						paper_l = t.value
						out_json.write('\n'+str(i*each_time)+'\n')
						out_json.write(json.dumps(paper_l))

						for paper in paper_l:
							out.write(str(paper))

					thread_l = []

