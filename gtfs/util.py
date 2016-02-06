from csv import reader

class TolerantDictReader(object):
	def __init__(self,stream):
		self.rd = reader(stream)
		header = self.rd.next()
		self.header = [fieldname.strip() for fieldname in header]

	def __iter__(self):
		return self

	def next(self):
		return dict(zip(self.header,self.rd.next()))