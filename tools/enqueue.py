from redis import Redis
from rq import  Queue

from tasks import add


conn = Redis('localhost', 6379)
q = Queue('default', connection=conn)
q.enqueue(add, 3,4)