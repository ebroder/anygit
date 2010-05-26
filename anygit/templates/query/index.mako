<%inherit file="../layouts/application.mako"/>

<%
  import pylons
  from routes.util import url_for
%>

You should probably run a query, or something.

<p>
<%self:form url="${url_for(controller='query', action='query_with_string')}">
${webhelpers.html.tags.text('query', title='SHA-1 hash to search for')}
${webhelpers.html.tags.select('limit', 10, [1, 5, 10, 20, 50, 100], title='Results per page')}
${webhelpers.html.tags.submit('submit', 'Query')}
</%self:form>
</p>
