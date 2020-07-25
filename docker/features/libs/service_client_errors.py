
class ClientServiceErrorID:
    SERVICE_NOT_READY= 0x01

class ServiceNotReady(Exception):
    def __init__(self):
        self.value = ClientServiceErrorID.SERVICE_NOT_READY
    def __str__(self):
        return repr(self.value)