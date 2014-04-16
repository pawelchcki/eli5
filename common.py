import threading

class QueueProcessor:
	def is_working(self):
		return getattr(self, 'working', True)

	def stop_work(self):
		self.working = False

	def start_work(self):
		self.working = True

	def launch(self, data_queue):
		thread = threading.Thread(target = self.process, args=[data_queue])
		thread.daemon = True
		return thread