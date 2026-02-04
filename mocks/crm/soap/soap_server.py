from spyne import Application, rpc, ServiceBase, Unicode, Iterable
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from wsgiref.simple_server import make_server

class CustomerService(ServiceBase):

    @rpc(_returns=Iterable(Unicode))
    def GetCustomers(ctx):
        return [
            "C003|Charlie Brown|charlie@example.com"
        ]

application = Application(
    [CustomerService],
    tns='crm.soap.service',
    in_protocol=Soap11(),
    out_protocol=Soap11()
)

wsgi_application = WsgiApplication(application)

if __name__ == '__main__':
    server = make_server('0.0.0.0', 8000, wsgi_application)
    server.serve_forever()
