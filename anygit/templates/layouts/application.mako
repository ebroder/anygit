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
	  <% messages = h.flash.pop_messages() %>
	  % if messages:
	  <ul id="flash-messages">
	    % for message in messages:
	    <li>${message}</li>
	    % endfor
	  </ul>
	  % endif

	  <% errors = h.error.pop_messages() %>
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
