import threading

class QueueProcessor:
	def launch(self, data_queue):
		return threading.Thread(target = self.process, args=[data_queue])