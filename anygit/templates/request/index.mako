<%inherit file="../layouts/application.mako"/>
<%
  import webhelpers.html as h
  import webhelpers.html.secure_form
  from routes.util import url_for
%>

Request to have your repository indexed.

<%self:form url="${url_for(controller='request', action='do_request')}">
<p> <label for="url">Git URL:</label> ${webhelpers.html.tags.text('url')} </p>
<p> ${webhelpers.html.tags.submit('submit', 'Index me please')} </p>
</%self:form>