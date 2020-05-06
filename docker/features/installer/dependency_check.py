import pdb
import pip3
print("checking dependency and performing installation")
def install(package):
    if hasattr(pip, 'main'):
        pip.main(['install', package])
    else:
        pip._internal.main(['install', package])

install('python-dotenv')
install('oauth2')
install('py2neo')