package istc.bigdawg.utils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.sun.jersey.api.model.AbstractResource;
import com.sun.jersey.api.model.AbstractResourceMethod;
import com.sun.jersey.api.model.AbstractSubResourceMethod;
import com.sun.jersey.server.impl.modelapi.annotation.IntrospectionModeller;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

@Path("/")
public class ResourceListingResource {
	
	@Path("show")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response showAll(@Context Application application,
			@Context HttpServletRequest request) {
		String basePath = request.getRequestURL().toString();
		// http://stackoverflow.com/questions/13484350/find-a-list-of-all-jersey-resource-methods-in-my-app

		ObjectNode root = JsonNodeFactory.instance.objectNode();
		ArrayNode resources = JsonNodeFactory.instance.arrayNode();

		root.put("resources", resources);

		for (Class<?> aClass : application.getClasses()) {
			if (isAnnotatedResourceClass(aClass)) {
				AbstractResource resource = IntrospectionModeller
						.createResource(aClass);
				ObjectNode resourceNode = JsonNodeFactory.instance.objectNode();
				String uriPrefix = resource.getPath().getValue();

				for (AbstractSubResourceMethod srm : resource
						.getSubResourceMethods()) {
					String uri = uriPrefix + "/" + srm.getPath().getValue();
					addTo(resourceNode, uri, srm, joinUri(basePath, uri));
				}

				for (AbstractResourceMethod srm : resource.getResourceMethods()) {
					addTo(resourceNode, uriPrefix, srm,
							joinUri(basePath, uriPrefix));
				}

				resources.add(resourceNode);
			}

		}

		return Response.ok().entity(root).build();
	}

	private void addTo(ObjectNode resourceNode, String uriPrefix,
			AbstractResourceMethod srm, String path) {
		if (resourceNode.get(uriPrefix) == null) {
			ObjectNode inner = JsonNodeFactory.instance.objectNode();
			inner.put("path", path);
			inner.put("verbs", JsonNodeFactory.instance.arrayNode());
			resourceNode.put(uriPrefix, inner);
		}

		((ArrayNode) resourceNode.get(uriPrefix).get("verbs")).add(srm
				.getHttpMethod());
	}

	public static String joinUri(String... parts) {
		StringBuilder result = new StringBuilder();
		for (String part : parts) {
			if (result.length() > 0
					&& result.charAt(result.length() - 1) == '/') {
				result.setLength(result.length() - 1);
			}
			if (result.length() > 0 && !part.startsWith("/")) {
				result.append('/');
			}
			result.append(part);
		}
		return result.toString();
	}

	private boolean isAnnotatedResourceClass(Class rc) {
		if (rc.isAnnotationPresent(Path.class)) {
			return true;
		}

		for (Class i : rc.getInterfaces()) {
			if (i.isAnnotationPresent(Path.class)) {
				return true;
			}
		}

		return false;
	}

}
