import httplib2
import sys

class QueryClient:
    version = 'auto'

    def __init__(self, scheme="http", host="192.168.99.100", port=8080):
        self.scheme = scheme
        self.host = host
        self.port = port

    def run_query(self, query):
        h = httplib2.Http()
        url = self.scheme + "://" + self.host + ":" + str(self.port) + "/bigdawg/query"
        print url
        resp, content = h.request(url,
                                  "POST", body=query, headers={'content-type':'application/json'})

        if sys.version_info >= (3, 0):
            content = content.decode("utf-8")

        if resp.status != 200:
            response = "Error:\n   Status: %d\n" %resp.status
            if content != '':
                response += "   Message: " + content + ""

            return response
        return content
