import memcache
import pprint
from random_words import RandomWords

#rw = RandomWords()
#word = rw.random_words(count=3)

#print '%s-%s-%s' % (word[0], word[1], word[2])

pp = pprint.PrettyPrinter(indent=4)

mc = memcache.Client(['127.0.0.1:11211'], debug=0)
pp.pprint(mc.get("task_list"))

