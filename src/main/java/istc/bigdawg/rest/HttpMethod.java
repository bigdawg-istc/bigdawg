package istc.bigdawg.rest;

public enum HttpMethod {
    GET, POST;

    public static HttpMethod parseMethod(String method) {
        switch(method.toUpperCase()) {
            case "POST":
                return HttpMethod.POST;
            case "GET":
                return HttpMethod.GET;
        }
        return null;
    }
}
