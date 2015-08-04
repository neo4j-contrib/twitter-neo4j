from twitterneo4j import application

if __name__ == "__main__":
    application.run(use_debugger=True, debug=True,
            use_reloader=True, host='0.0.0.0')
