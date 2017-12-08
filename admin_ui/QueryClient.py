import httplib2

class QueryClient:
    version = 'auto'

    def run_query(self, query):
        h = httplib2.Http()
        resp, content = h.request("http://localhost:8080/bigdawg/query",
                                  "POST", body=query, headers={'content-type':'application/json'})
        if resp.status != 200:
            response = "Error:\n   Status: %d\n" %resp.status
            if content != '':
                response += "   Message: " + content + ""

            return response
        return content
