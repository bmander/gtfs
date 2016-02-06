from csv import reader

class TolerantDictReader(object):
	def __init__(self,stream):
		self.rd = reader(stream)
		header = self.rd.next()
		self.header = [fieldname.strip().decode("utf-8") for fieldname in header]

	def __iter__(self):
		return self

	def next(self):
		return dict(zip(self.header,[unicode(x,"utf-8") for x in self.rd.next()]))