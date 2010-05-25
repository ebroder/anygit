<%inherit file="../layouts/application.mako"/>

<%
  import pylons
  from routes.util import url_for
%>

You should probably run a query, or something.

<p>
<form type="GET" action="${url_for(controller='query', action='query_with_string')}" />
<input type="text" name="query" value="${c.queried_id}" />
<input type="submit" value="Update query" />
</form>
</p>
