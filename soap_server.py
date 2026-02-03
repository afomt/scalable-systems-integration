from spyne import Application, rpc, ServiceBase, Unicode, Integer, Iterable
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from wsgiref.simple_server import make_server

class CustomerSOAPService(ServiceBase):
    @rpc(Unicode, _returns=Unicode)
    def AddCustomer(ctx, name):
        """Mock SOAP call to add a customer [cite: 20]"""
        return f"Customer {name} added successfully with ID: SOAP-123"

application = Application([CustomerSOAPService], 'moko.crm.soap',
                          in_protocol=Soap11(validator='lxml'),
                          out_protocol=Soap11())

if __name__ == '__main__':
    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    print("SOAP Server running on http://0.0.0.0:8000")
    server.serve_forever()