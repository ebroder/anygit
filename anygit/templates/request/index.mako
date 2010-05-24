<%inherit file="../layouts/application.mako"/>
<%
  import webhelpers.html as h
  import webhelpers.html.secure_form
  from routes.util import url_for
%>

Request to have your repository indexed.

<%self:form url="${url_for(controller='index', action='do_request')}">
${webhelpers.html.tags.text('url')}
${webhelpers.html.tags.submit('submit', 'Index')}
</%self:form>
