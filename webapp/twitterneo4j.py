from flask import Flask
from flask import flash
from flask import url_for 
from flask import request
from flask import render_template
from flask import redirect
from flask_oauthlib.client import OAuth
from flask import session
import create_task

application = Flask(__name__)
application.debug = True

application.secret_key = 'super secret key'
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

    if loggedin:
        n4j_url = create_task.create_task(loggedin)
        return render_template('show_link.html', url=n4j_url)
    else:
        return render_template('home.html')

@application.route('/oauth-authorized')
def oauth_authorized():
    next_url = request.args.get('next') or url_for('index')
    resp = twitter.authorized_response()
    if resp is None:
        flash(u'You denied the request to sign in.')
        return redirect(next_url)

    session['twitter_token'] = (
        resp['oauth_token'],
        resp['oauth_token_secret']
    )
    session['twitter_user'] = resp['screen_name']

    flash('You were signed in as %s' % resp['screen_name'])
    return redirect(next_url)

if __name__ == "__main__":
    application.run(use_debugger=True, debug=True,
            use_reloader=True, host='0.0.0.0')
