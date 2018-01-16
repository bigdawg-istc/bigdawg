import httplib2

class QueryClient:
    version = 'auto'

    def __init__(self, scheme="http", host="192.168.99.100", port=8080):
        self.scheme = scheme
        self.host = host
        self.port = port

    def run_query(self, query):
        h = httplib2.Http()
        resp, content = h.request(self.scheme + "://" + self.host + ":" + str(self.port) + "/bigdawg/query",
                                  "POST", body=query, headers={'content-type':'application/json'})
        if resp.status != 200:
            response = "Error:\n   Status: %d\n" %resp.status
            if content != '':
                response += "   Message: " + content + ""

            return response
        return content
