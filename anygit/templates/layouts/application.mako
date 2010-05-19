<%
  from pylons import url
%>

<html>
  <head>
    <link href="${url('/static/style.css')}" rel="stylesheet" type="text/css" />
  </head>
    <body>
        <div class="header">
            ${self.header()}
        </div>

	<div class="flash">
	  <%
	     messages = h.flash.pop_messages()
	     if kwargs['flash_now']:
               messages.append(kwargs['flash_now'])
          %>
	  % if messages:
	  <ul id="flash-messages">
	    % for message in messages:
	    <li>${message}</li>
	    % endfor
	  </ul>
	  % endif

	  <%
	     errors = h.error.pop_messages()
	     if kwargs['error_now']:
               errors.append(kwargs['error_now'])
          %>
	  % if errors:
	  <ul id="error-messages">
	    % for error in errors:
	    <li>${error}</li>
	    % endfor
	  </ul>
	  % endif
	</div>

        ${self.body()}

        <div class="footer">
            ${self.footer()}
        </div>
    </body>
</html>

<%def name="header()">
    Hello world
</%def>

<%def name="footer()">
    Brought to you by the anyg.it team
</%def>

<%def name="form(url)">
${webhelpers.html.secure_form.secure_form(url)}
${caller.body()}
${webhelpers.html.tags.end_form()}
</%def>

<%def name="link_to_object(obj)">
<a href="${h.get_url(obj)}">${obj.id}</a>
</%def>

<%def name="link_to_repo(repo, obj)">
<a href="${repo.url}">${repo.url}</a>
</%def>

<%def name="link_to_view(repo, obj)">
<% v = h.get_view_url_for(repo, obj) %>
% if v:
<a href="${v}">${repo.url}</a>
% else:
<b>${repo.url}</b>
% endif
</%def>
