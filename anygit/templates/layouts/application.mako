<%
  from pylons import url
%>

<html>
  <head>
    <title>anygit</title>
    <link href="${url('/static/style.css')}" rel="stylesheet" type="text/css" />
    <link rel="SHORTCUT ICON" href="/static/anygit.ico"/>
    <link rel="icon" type="image/vnd.microsoft.icon" href="/static/anygit.ico">
  </head>
    <body>
        <div class="header">
          ${self.header()}
        </div>

        <%
           messages = h.flash.pop_messages()
           if kwargs['flash_now']:
             messages.append(kwargs['flash_now'])
        %>
        % if messages:
        <div class="flash">
          <ul id="flash-messages">
            % for message in messages:
            <li>${message}</li>
            % endfor
          </ul>
        </div>
        % endif

        <%
           errors = h.error.pop_messages()
           if kwargs['error_now']:
             errors.append(kwargs['error_now'])
        %>
        % if errors:
        <div class="flash">
          <ul id="error-messages">
            % for error in errors:
            <li>${error}</li>
            % endfor
          </ul>
        </div>
        % endif

        <div class="body">
          ${self.body()}
        </div>

        <div class="footer">
          ${self.footer()}
        </div>
    </body>
</html>

<%def name="header()">
    Welcome to <a href="/">anygit</a>, indexing the world's git repositories one at a time.
</%def>

<%def name="footer()">
    brought to you by the <a href="/about">anyg.it team</a>
</%def>

<%def name="form(url)">
${webhelpers.html.secure_form.secure_form(url)}
${caller.body()}
${webhelpers.html.tags.end_form()}
</%def>

<%def name="link_to_object(obj)">
<%
if not isinstance(obj, basestring):
  obj = obj.id
%>
<a href="${h.get_url(obj)}">${obj}</a>
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
