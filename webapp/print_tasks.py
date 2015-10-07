import memcache
import pprint

pp = pprint.PrettyPrinter(indent=4)

mc = memcache.Client(['127.0.0.1:11211'], debug=0)
pp.pprint(mc.get("task_list"))

