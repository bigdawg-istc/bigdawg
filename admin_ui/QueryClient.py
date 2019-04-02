import httplib2
import json

class QueryClient:
    version = 'auto'

    def __init__(self, scheme="http", host="192.168.99.100", port=8080):
        self.scheme = scheme
        self.host = host
        self.port = port

    def run_query(self, query):
        h = httplib2.Http()
        url = self.scheme + "://" + self.host + ":" + str(self.port) + "/bigdawg/query"
        print(url)
        try:
            resp, content = h.request(url,
                                      "POST", body=query, headers={'content-type':'application/json'})
            content = content.decode("utf-8")

            if resp.status != 200:
                response = "Error:\n   Status: %d\n" %resp.status
                if content != '':
                    response += "   Message: " + content + ""

                return json.JSONEncoder().encode({ "success": False, "error": response})

            return json.JSONEncoder().encode({ "success": True, "content": content })
        except Exception as e:
            return json.JSONEncoder().encode({ "success": False, "error": str(e) })
