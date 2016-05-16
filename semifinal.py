import ujson as json
import time
import urllib
import urllib2
import threading
import os
import commands  
from flask import Flask,jsonify,request,abort
import socket
import threading,Queue
import gevent
url = 'http://oxfordhk.azure-api.net/academic/v1.0/evaluate'
expr_max_length = 1750
para = dict()
para['attributes'] = 'Id,AA.AuId,AA.AfId,J.JId,C.CId,RId,F.FId,CC'
para['subscription-key'] = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
para['count'] = '900000'

host = 'oxfordhk.azure-api.net'
port = 80
domain = 'academic/v1.0/evaluate'


total_sock = 5 
s_q = Queue.Queue()

class Paper_Entity(object):

	def __init__(self, entity):
		self.entity = entity
		self.init_PE()

	def init_PE(self):
		self.get_Id()
		self.get_AuId_AfId_list()

		self.get_FId_list()
		self.get_CId_list()

	def get_Id(self):
		self.Id = self.entity.get('Id')
		return self.Id

	def get_AuId_AfId_list(self):
		AA = self.entity.get('AA',{})
		self.AuId = []
		self.AfId = []
		for d in AA:
			self.AuId.append(d.get('AuId'))
			self.AfId.append(d.get('AfId'))
		return self.AuId,self.AfId 


	def get_FId_list(self):
		F_l = self.entity.get('F',{})
		self.FId = []
		for F in F_l:
			self.FId.append(F.get('FId'))
		return self.FId

	def get_CId_list(self):
		C_l = self.entity.get('C')
		J_l = self.entity.get('J')
		self.CId = []
		if C_l:
			self.CId.append(C_l.get('CId'))
		if J_l:
			self.CId.append(J_l.get('JId'))
		return self.CId

def create_socket():
	s = time.time()
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((host,port))
	return sock

class socket_class():
	def __init__(self):
		self.sock = create_socket()
		self.time = time.time()
	def get_sock(self):
		now = time.time()
		if now - self.time>60:
				self.sock.close()
				self.sock = create_socket()
		self.time = now
		return self.sock
	def __del__(self):
		self.sock.close()


def get_ans(para, i=0):
	global host, port, domain
	try:
		sock_c = s_q.get(block=False)
	except:
		sock_c = socket_class()
	sock = sock_c.get_sock()
	para = '&'.join(['='.join(item) for item in para.items()])

	request_str = '''GET /%s?%s HTTP/1.1\r\nHost: %s\r\n\r\n''' %(domain, para, host)
	sock.send(request_str)
	total_data=[]
	#sock.settimeout(5)
	data = sock.recv(150)
	length = int(data.split('\n')[1].split(' ')[1])
	header_len = len(data.split('\r\n\r\n')[0])
	total_data.append(data[header_len+4:])
	length = length-(len(data) - header_len - 4)

	while 1:
		if length>0:
			data = sock.recv(min(length,8192))
			length -= len(data)
			total_data.append(data)
		else:
			break
	ans = ''.join(total_data)
	s_q.put(sock_c)
	return ans


for i in range(total_sock):
	s_q.put(socket_class())

def heart_beat():
	global total_sock
	para = dict()
	para['attributes'] = 'Id,AA.AuId,AA.AfId,J.JId,C.CId,RId,F.FId,CC'
	para['subscription-key'] = 'f7cc29509a8443c5b3a5e56b0e38b5a6'
	para['count'] = '70'
	para['expr'] = 'Y>2000'
	count = 0
	while True:
		if s_q.qsize()>total_sock*2:
			sock_c = s_q.get()
			del sock_c
		get_ans(para)
		print 'heart_beat'
		count+=1
		if count > total_sock:
			time.sleep(60/total_sock+1)


threading.Thread(target=heart_beat).start()


import httplib
def get_ans_http(para):
	ans = 0
	src = '&'.join(['='.join(item) for item in para.items()])
	httpClient = httplib.HTTPConnection('oxfordhk.azure-api.net', 80)
	httpClient.request('GET', '/academic/v1.0/evaluate?%s'%(src,))
	response = httpClient.getresponse()
	ans = response.read()
	httpClient.close()
	return ans


#@profile
def get_entity(expr,attr='Id,AA.AuId,AA.AfId,J.JId,C.CId,RId,F.FId,CC',thl=[]):
	t_l = []
	q = Queue.Queue()
	entities = []
	pe_l = []
	try:
		for i in xrange(2):
			t_l.append(threading.Thread(target=get_entity_one,args=(expr,q,i,attr)))
			t_l[i].start()
		gevent.joinall(thl)
		ans = q.get()
		entities = json.loads(ans)['entities']
	except:
		para['expr'] = expr
		para['attributes'] = attr
		ans = get_ans_http(para)
		entities = json.loads(ans)['entities']

	for en in entities:
		pe = Paper_Entity(en)
		pe_l.append(pe)
	return pe_l


#@profile
def get_entity_one(expr,q,i,attr='Id,AA.AuId,AA.AfId,J.JId,C.CId,RId,F.FId,CC'):
	para['expr'] = expr
	para['attributes'] = attr
	ans = get_ans(para,i)
	q.put(ans)


def Or_expr_Id(Id_l):
	length = len(Id_l)
	if length == 1:
		return ['Id=%s'%(Id_l[0],)]
	else:
		ans_l = []
		ans = 'Or(Id=%s,Id=%s)'%(Id_l[0],Id_l[1])
		for i in range(2, length):
			t = 'Or(%s,Id=%s)'%(ans,Id_l[i])
			if len(t)>expr_max_length-20:
				ans_l.append(ans)
				ans = 'Id=%s'%(Id_l[i],)
			else:
				ans = t
		ans_l.append(ans)
		return ans_l


#@profile
def Id_rRId_Id(Id1, Id2, Id1_en, Id2_rRId_Id_l, before=None, after=None):
	ans = []
	middle = set(Id2_rRId_Id_l).intersection(Id1_en.entity.get('RId',[]))
	if before:
		ans =[ (before, Id1, m, Id2)  for m in middle ]
	elif after:
		ans =[ (Id1, m, Id2, after)  for m in middle ]
	else:
		ans =[ (Id1, m, Id2)  for m in middle ]
	return ans


#@profile
def Id_CFAu_Id(Id1, Id2, Id1_en, Id2_en, before=None, after=None):
	ans = []
	if before:
		if Id1_en.CId == Id2_en.CId and Id1_en.CId!=[]:
			ans += [ (before, Id1, Id1_en.CId[0], Id2) ]

		middle = set(Id1_en.FId).intersection(Id2_en.FId)
		ans += [ (before, Id1,m,Id2)  for m in middle ]

		middle = set(Id1_en.AuId).intersection(Id2_en.AuId)
		ans += [ (before, Id1,m,Id2)  for m in middle ]
	elif after:
		if Id1_en.CId == Id2_en.CId and Id1_en.CId!=[]:
			ans += [ (Id1, Id1_en.CId[0], Id2, after) ]

		middle = set(Id1_en.FId).intersection(Id2_en.FId)
		ans += [ (Id1,m,Id2,after)  for m in middle ]

		middle = set(Id1_en.AuId).intersection(Id2_en.AuId)
		ans += [ (Id1,m,Id2,after)  for m in middle ]
	elif before==None and after==None:

		if Id1_en.CId == Id2_en.CId and Id1_en.CId!=[]:
			ans += [ (Id1, Id1_en.CId[0], Id2) ]

		middle = set(Id1_en.FId).intersection(Id2_en.FId)
		ans += [ (Id1,m,Id2)  for m in middle ]

		middle = set(Id1_en.AuId).intersection(Id2_en.AuId)
		ans += [ (Id1,m,Id2)  for m in middle ]
	return ans

#@profile
def get_Id_Id(Id1, Id2, Id1_en, Id2_en):
	ans = []

	Id1_RId_en_l = []
	Id1_RId = Id1_en.entity.get('RId',[])
	if Id1_RId != []:
		expr_l = Or_expr_Id(Id1_RId)
		for expr in expr_l:
			Id1_RId_en_l += get_entity(expr=expr,attr="Id,J.JId,C.CId,F.FId,RId,AA.AuId")

	"""Id xxx Id Id"""
	expr_l = []
	if Id1_en.AuId:
		expr_l += Or_expr_AuId(Id1_en.AuId)
	if Id1_en.FId:
		expr_l.append(Or_expr_FId(Id1_en.FId))
	if Id1_en.CId:
		expr_l.append('Or(Composite(C.CId=%s),Composite(J.JId=%s))'%(Id1_en.CId[0],Id1_en.CId[0]))

	for Id1_RId_en in Id1_RId_en_l:
		if Id1_RId_en.entity.get('RId'):
			expr_l += Or_expr_Id(Id1_RId_en.entity.get('RId'))

	thread_l = []
	if expr_l:
		expr_l = Or_expr(expr_l)
		for expr in expr_l:
			expr = 'And(%s,RId=%s)'%(expr,Id2)
			gt = gevent.spawn( get_entity, expr, 'Id,AA.AuId,F.FId,J.JId,C.CId') 
			thread_l.append(gt)

	"""one hop"""
	if Id1_RId.count(Id2) > 0:
		ans += [ (Id1,Id2) ]

	"""two hop"""
	"""id id id"""

	ans += Id_RId_Id(Id1, Id2, Id1_RId_en_l)

	"""id CFAu Id"""
	ans += Id_CFAu_Id(Id1, Id2, Id1_en, Id2_en)

	"""three hop"""
	"""Id1 RId CFAu Id2"""

	for Id1_RId_en in Id1_RId_en_l:
		ans += Id_CFAu_Id(Id1_RId_en.Id, Id2, Id1_RId_en, Id2_en, before=Id1)
		
	"""Id1 CFRAu Id Id2"""
	gevent.joinall(thread_l)
	for t in thread_l:
		entities = t.value
		for en in entities:
			ans += Id_CFAu_Id(Id1, en.Id, Id1_en, en, after=Id2)
			ans += Id_RId_Id(Id1, en.Id, Id1_RId_en_l, after=Id2)
	return list(set(ans))


#@profile
def get_Id_Id_CC(Id1, Id2, Id1_en, Id2_en):
	ans = []
	Id1_RId_en_l = []
	Id1_RId = Id1_en.entity.get('RId',[])
	Id2_rRId_en_l = []
	if Id1_RId != []:
		entities = get_entity(expr='Or(%s,RId=%s)' %(Or_expr_Id(Id1_RId)[0], Id2),attr="Id,J.JId,C.CId,F.FId,RId,AA.AuId,AA.AfId")
		for en in entities:
			if en.Id in Id1_RId:
				Id1_RId_en_l.append(en)
			if Id2 in en.entity.get('RId',[]):
				Id2_rRId_en_l.append(en)

	"""one hop"""
	if Id1_RId.count(Id2) > 0:
		ans += [ (Id1,Id2) ]

	"""two hop"""
	"""id id id"""
	Id2_rRId_Id_l = []
	for en in Id2_rRId_en_l:
		Id2_rRId_Id_l.append(en.Id)
	ans += Id_rRId_Id(Id1, Id2, Id1_en, Id2_rRId_Id_l)


	"""id CFAu Id"""
	ans += Id_CFAu_Id(Id1, Id2, Id1_en, Id2_en)


	"""three hop"""
	"""Id1 RId xxx Id2"""

	for Id1_RId_en in Id1_RId_en_l:
		after_par = Id_rRId_Id(Id1_RId_en.Id, Id2, Id1_RId_en, Id2_rRId_Id_l)
		ans += [(Id1,)+af for af in after_par]

		after_par = Id_CFAu_Id(Id1_RId_en.Id, Id2, Id1_RId_en, Id2_en)
		ans += [(Id1,)+af for af in after_par]
	"""Id1 CFAu Id Id2"""
	for Id2_rRId_en in Id2_rRId_en_l:
		before_par = Id_CFAu_Id(Id1, Id2_rRId_en.Id, Id1_en, Id2_rRId_en)
		ans += [bf+(Id2,) for bf in before_par]

	return list(set(ans))


#@profile
def Or_expr(expr_l):
	length = len(expr_l)
	if length == 1:
		return [expr_l[0]]
	else:
		ans_l = []
		ans = expr_l[0]
		for i in range(1, length):
			t = 'Or(%s,%s)'%(ans, expr_l[i])
			if len(t)>expr_max_length:
				ans_l.append(ans)
				ans = expr_l[i]
			else:
				ans = t
		ans_l.append(ans)
		return ans_l


#@profile
def Or_expr_FId(FId_l):
	length = len(FId_l)
	if length == 1:
		return 'Composite(F.FId=%s)'%(FId_l[0],)
	else:
		ans = 'Or(Composite(F.FId=%s),Composite(F.FId=%s))'%(FId_l[0],FId_l[1])
		for i in xrange(2, length):
			ans = 'Or(%s,Composite(F.FId=%s))'%(ans,FId_l[i])
		return ans


#@profile
def Or_expr_AuId(AuId_l):
	length = len(AuId_l)
	if length == 1:
		return ['Composite(AA.AuId=%s)'%(AuId_l[0], ) ]
	else:
		ans_l = []
		ans = 'Composite(AA.AuId=%s)'%(AuId_l[0],)
		for i in xrange(1, length):
			t = 'Or(%s,Composite(AA.AuId=%s))'%(ans,AuId_l[i])
			if len(t)>expr_max_length:
				ans_l.append(ans) 
				ans = 'Composite(AA.AuId=%s)'%(AuId_l[i],)
			else:
				ans = t
		ans_l.append(ans)
	return ans_l


#@profile
def Id_RId_Id(Id1, Id2, Id1_RId_en_l, before=None, after=None):
	if before:
		ans = [ (before, Id1, Id1_RId_en.Id, Id2)  for Id1_RId_en in Id1_RId_en_l if Id2 in Id1_RId_en.entity.get('RId',[])]
	elif after:
		ans = [ (Id1, Id1_RId_en.Id, Id2, after)  for Id1_RId_en in Id1_RId_en_l if Id2 in Id1_RId_en.entity.get('RId',[])]
	elif before == None and after == None:
		ans = [ (Id1, Id1_RId_en.Id, Id2)  for Id1_RId_en in Id1_RId_en_l if Id2 in Id1_RId_en.entity.get('RId',[])]
	return ans

#@profile
def get_Id_AuId(Id1, AuId2, Id1_en, AuId2_write_en_l):
	ans = []
	Id1_AuAf_d = {}
	AuId2_Af_l = []
	for en in AuId2_write_en_l:
		l = len(en.AuId)
		for i in xrange(l):
			if en.AuId[i] == AuId2:
				AuId2_Af_l.append(en.AfId[i])

	Id1_RId = Id1_en.entity.get('RId',[])
	Id1_Au = Id1_en.AuId
	for Au in Id1_Au:
		Id1_AuAf_d[Au] = []
	Id1_RId_en_l = []
	Id1_Au_en_l = []
	expr_l = []
	if Id1_RId:
		expr_l += Or_expr_Id(Id1_RId)
	if Id1_Au:
		expr_l += Or_expr_AuId(Id1_Au)
	expr_l = Or_expr(expr_l)
	entities = []
	for expr in expr_l:
		entities += get_entity(expr=expr, attr = "Id,J.JId,C.CId,F.FId,RId,AA.AuId,AA.AfId" )
	
	for en in entities:
		if en.Id in Id1_RId:
			Id1_RId_en_l.append(en)
		if len(set(en.AuId).intersection(Id1_Au))>0:
			Id1_Au_en_l.append(en)

	for en in Id1_Au_en_l:
		l = len(en.AuId)
		for i in xrange(l):
			if en.AuId[i] in Id1_Au:
				Id1_AuAf_d[en.AuId[i]].append(en.AfId[i])


	"""one hop"""
	if AuId2 in Id1_en.AuId:
		ans.append((Id1,AuId2))

	"""two hop"""
	"""Id Id AuId"""
	AuId2_write_Id = []
	for AuId2_write_en in AuId2_write_en_l:
		AuId2_write_Id.append(AuId2_write_en.Id)

	middle = set(Id1_RId).intersection(AuId2_write_Id)
	ans += [ (Id1, m, AuId2) for m in middle ]

	"""three hop"""
	"""Id Au Af Au"""
	for Au in Id1_Au:
		middle = set(Id1_AuAf_d[Au]).intersection(AuId2_Af_l)
		middle.discard(None)
		ans += [ (Id1, Au, m, AuId2) for m in middle ]

	"""Id xxx Id Au"""
	for AuId2_write_en in AuId2_write_en_l:
		ans += Id_CFAu_Id(Id1, AuId2_write_en.Id, Id1_en, AuId2_write_en, after=AuId2)
		ans += Id_RId_Id(Id1, AuId2_write_en.Id, Id1_RId_en_l, after=AuId2)

	return list(set(ans))

#@profile
def get_AuId_Id(AuId, Id2, AuId_write_en_l, Id2_en):
	ans = []
	"""pre Au id id id"""
	expr_l = []
	for AuId_write_en in AuId_write_en_l:
		if AuId_write_en.entity.get('RId'):
			expr_l+=Or_expr_Id(AuId_write_en.entity.get('RId'))
	thread_l = []
	if expr_l:
		expr_l = Or_expr(expr_l)
		for expr in expr_l:
			expr = 'And(RId=%s,%s)'%(Id2, expr)
			gt = gevent.spawn(get_entity, expr, 'Id')
			thread_l.append(gt)

	AuId_Af_l = []
	for en in AuId_write_en_l:
		l = len(en.AuId)
		for i in xrange(l):
			if en.AuId[i] == AuId:
				AuId_Af_l.append(en.AfId[i])

	Id2_Au = Id2_en.AuId
	Id2_AuAf_d = {}
	for Au in Id2_Au:
		Id2_AuAf_d[Au] = []

	if Id2_Au != []:
		expr_l = Or_expr_AuId(Id2_Au)
		Id2_Au_en_l = []
		for expr in expr_l:
			Id2_Au_en_l += get_entity(expr=expr,attr="AA.AuId,AA.AfId",thl=thread_l)
	

	for en in Id2_Au_en_l:
		l = len(en.AuId)
		for i in xrange(l):
			if en.AuId[i] in Id2_Au:
				Id2_AuAf_d[en.AuId[i]].append(en.AfId[i])


	"""one hop"""
	if AuId in Id2_en.AuId:
		ans += [ (AuId, Id2) ]

	"""two hop"""
	"""AuId Id Id"""
	for AuId_write_en in AuId_write_en_l:
		if Id2 in AuId_write_en.entity.get('RId',[]):
			ans += [ (AuId, AuId_write_en.Id, Id2) ]

	"""three hop"""
	"""Au Af Au Id"""
	for Au in Id2_Au:
		middle = set(Id2_AuAf_d[Au]).intersection(AuId_Af_l)
		middle.discard(None)
		ans += [ (AuId, m, Au, Id2) for m in middle ]

	"""Au Id CFAuId Id"""
	for AuId_write_en in AuId_write_en_l:
		ans += Id_CFAu_Id(AuId_write_en.Id, Id2, AuId_write_en, Id2_en, before=AuId)
	"""Au Id Id Id"""

	gevent.joinall(thread_l)
	for t in thread_l:
		entities = t.value
		for AuId_write_en in AuId_write_en_l:
			for en in entities:
				if en.Id in AuId_write_en.entity.get('RId',[]):
					ans.append((AuId, AuId_write_en.Id, en.Id, Id2))

	return list(set(ans))

#@profile
def get_AuId_AuId(AuId1, AuId2, AuId1_write_en_l, AuId2_write_en_l):
	ans = []
	
	AuId1_Af_l = []
	AuId2_Af_l = []
	AuId1_write_Id = []
	AuId2_write_Id = []
	for en in AuId1_write_en_l:
		if AuId1 in en.AuId:
			index = en.AuId.index(AuId1)
			AuId1_Af_l.append(en.AfId[index])
			AuId1_write_Id.append(en.Id)
	for en in AuId2_write_en_l:
		if AuId2 in en.AuId:
			index = en.AuId.index(AuId2)
			AuId2_Af_l.append(en.AfId[index])
			AuId2_write_Id.append(en.Id)
	"""one hop:None"""
	"""two hop"""
	"""Au Af Au"""

	middle = set(AuId2_Af_l).intersection(AuId1_Af_l)
	middle.discard(None)
	ans += [ (AuId1, m, AuId2) for m in middle ]
	"""Au Id Au"""
	middle = set(AuId1_write_Id).intersection(AuId2_write_Id)
	ans += [ (AuId1, m, AuId2) for m in middle ]
	"""three hop"""
	"""Au Id Id Au"""
	for AuId1_write_en in AuId1_write_en_l:
		middle = set(AuId1_write_en.entity.get('RId',[])).intersection(AuId2_write_Id)
		ans += [ (AuId1, AuId1_write_en.Id, m, AuId2) for m in middle ]
	return list(set(ans))


#@profile
def get_all_possible_ans(Id1,Id2):
	entities = get_entity(expr='Or(Or(Or(Composite(AA.AuId=%s),Composite(AA.AuId=%s)),Id=%s),Id=%s)' % (Id1, Id2, Id1, Id2))
	Id1_en = None
	Id2_en = None
	
	AuId1_write_en_l = []
	AuId2_write_en_l = []
	Id2_rRId_en_l = []
	for en in entities:
		if en.Id == Id1:
			Id1_en = en
		if en.Id == Id2:
			Id2_en = en
		if Id1 in en.AuId:
			AuId1_write_en_l.append(en)
		if Id2 in en.AuId:
			AuId2_write_en_l.append(en)

	ans = []
	if Id1_en and Id2_en:
		if (Id1_en.AuId or Id1_en.entity.get('RId',[])) and (Id2_en.AuId or Id2_en.entity.get('RId',[])):	
			print 'Id-Id'
			if Id2_en.entity.get('CC',0)>1000:
				print 'CC>1000'
				ans+=get_Id_Id(Id1, Id2, Id1_en, Id2_en)
			else:
				print 'CC<1000'
				ans += get_Id_Id_CC(Id1, Id2, Id1_en, Id2_en)
	if AuId1_write_en_l and Id2_en:
		if Id2_en.AuId or Id2_en.entity.get('RId',[]):
			ans+=get_AuId_Id(Id1, Id2, AuId1_write_en_l, Id2_en)
			print 'AuId-Id'
	if Id1_en and AuId2_write_en_l:
		if Id1_en.AuId or Id1_en.entity.get('RId',[]):
			print 'Id-AuId'
			ans += get_Id_AuId(Id1, Id2, Id1_en, AuId2_write_en_l)
	if AuId1_write_en_l and AuId2_write_en_l:
		print 'AuId-AuId'
		ans +=get_AuId_AuId(Id1, Id2, AuId1_write_en_l, AuId2_write_en_l)

	print 1
	return ans


app = Flask(__name__)
#@app.route('/api.chinacloudapp.cn/vvv')
@app.route('/semifinal')
def cca():
	Id1 =  request.args.get("id1")
	Id2 = request.args.get('id2')
	Id1 = int(Id1)
	Id2 = int(Id2)
	print Id1,Id2
	ans = get_all_possible_ans(Id1,Id2)
	return json.dumps(ans)


if __name__ =='__main__':
	app.run(host='0.0.0.0',port=5000,debug=False)


