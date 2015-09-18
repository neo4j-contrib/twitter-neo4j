from flask import Flask
from flask import jsonify
from flask.json import dumps
from flask import flash
from flask import url_for 
from flask import request
from flask import render_template
from flask import redirect
from flask_oauthlib.client import OAuth
from flask import session
import logging

from py2neo import Graph
from py2neo.packages.httpstream import http
from py2neo.packages.httpstream import SocketError


import create_task

TWITTER_CONSUMER_KEY = 'sAHNiOdNdGlJH0tzHmYa2kTKU'
TWITTER_CONSUMER_SECRET = 'RwQIxMW58VgLJ1LGU6HHRqYCd2hnGVQvSP6Ogr7Zw7HfCSh1Nj'

application = Flask(__name__)
application.debug = True

application.secret_key = '32079fgalkjnERER134NBZ><'
application.config['SESSION_TYPE'] = 'filesystem'

oauth = OAuth()

twitter = oauth.remote_app('twitter',
    base_url='https://api.twitter.com/1/',
    request_token_url='https://api.twitter.com/oauth/request_token',
    access_token_url='https://api.twitter.com/oauth/access_token',
    authorize_url='https://api.twitter.com/oauth/authenticate',
    consumer_key='sAHNiOdNdGlJH0tzHmYa2kTKU',
    consumer_secret='RwQIxMW58VgLJ1LGU6HHRqYCd2hnGVQvSP6Ogr7Zw7HfCSh1Nj',
    access_token_method='POST'
)

logging.getLogger("py2neo.cypher").setLevel(logging.CRITICAL)

@application.route("/login", methods=['GET', 'POST'])
def login():
    return twitter.authorize(callback=url_for('oauth_authorized',
        next=request.args.get('next') or request.referrer or None))

@application.route("/", methods=['GET', 'POST'])
def index():
    if 'twitter_user' in session:
      loggedin = session['twitter_user']
    else:
      loggedin = False

    if 'neo4j_url' in session:
      neo4j_url = session['neo4j_url']
    else:
      neo4j_url = False

    if loggedin:
        return render_template('home.html', check_for_url=True, user=loggedin)
    else:
        return render_template('home.html')

@application.route("/get_n4j_url", methods=['GET'])
def get_neo4j_url():
  global TWITTER_CONSUMER_KEY
  global TWITTER_CONSUMER_SECRET
  response_dict = {}

  if 'neo4j_url' in session:
    response_dict['neo4j_url'] = session['neo4j_url']
  elif 'twitter_user' in session:
    n4j_url = create_task.create_task(screen_name=session['twitter_user'], consumer_key=TWITTER_CONSUMER_KEY, consumer_secret=TWITTER_CONSUMER_SECRET, user_key=session['oauth_token'], user_secret=session['oauth_token_secret'])
    session['neo4j_url'] = n4j_url
    response_dict['neo4j_url'] = n4j_url

  return jsonify(**response_dict)

@application.route('/oauth-authorized')
def oauth_authorized():
    next_url = request.args.get('next') or url_for('index')
    resp = twitter.authorized_response()
    if resp is None:
        flash(u'You denied the request to sign in.')
        return redirect(next_url)

    session['oauth_token'] = resp['oauth_token']
    session['oauth_token_secret'] = resp['oauth_token_secret']

    session['twitter_user'] = resp['screen_name']

    #flash('You were signed in as %s' % resp['screen_name'])
    return redirect(next_url)

@application.route("/neo4j-node-count", methods=['GET', 'POST'])
def exec_neo4j_node_count():
    response_dict = {}
    log = logging.getLogger("httpstream")
    log.addHandler(logging.NullHandler())

    if not 'neo4j_url' in session:
      raise Exception('no neo4j url defined in session when exec cypher')

    http.socket_timeout = 5

    try:
      graph = Graph("%s/db/data/" % session['neo4j_url'])
      cntCypher = 'MATCH (a) WITH DISTINCT LABELS(a) AS temp, ' + \
                  'COUNT(a) AS tempCnt UNWIND temp AS label ' + \
                  'RETURN label, SUM(tempCnt) AS cnt'

      res = graph.cypher.execute(cntCypher)
      for record in res:
        response_dict['count_%s' % record.label] = record.cnt
    except SocketError:
      raise Exception("none")
 
    return jsonify(**response_dict)

@application.route("/exec-query", methods=['GET'])
def exec_neo4j_query():
    res_list = []
    response_dict = { 'results': res_list }

    http.socket_timeout = 5

    query = request.args.get('query')
    if query == 'mentions':
      columns = ('screen_name', 'count')
      mentionsCypher = 'MATCH (u:User {screen_name:{sn}})-[:POSTS]->(t:Tweet)-[:MENTIONS]->(m:User) ' + \
                       'RETURN m.screen_name AS screen_name, COUNT(m.screen_name) AS count ORDER BY count DESC LIMIT 10'
      graph = Graph("%s/db/data/" % session['neo4j_url'])
      res = graph.cypher.execute(mentionsCypher, {'sn': session['twitter_user'] })
      for record in res:
        res_list.append(dict(zip(columns, record)))
    elif query == 'tags':
      columns = ('tag', 'count')
      mentionsCypher = 'MATCH (h:Hashtag)-[:TAGS]->(t:Tweet) WITH h, COUNT(h) AS Hashtags ORDER BY Hashtags DESC LIMIT 10 RETURN h.name AS tag, Hashtags AS count'
      graph = Graph("%s/db/data/" % session['neo4j_url'])
      res = graph.cypher.execute(mentionsCypher)
      for record in res:
        res_list.append(dict(zip(columns, record)))

    return jsonify(**response_dict)

if __name__ == "__main__":
    application.run(use_debugger=True, debug=True,
            use_reloader=True, host='0.0.0.0')
