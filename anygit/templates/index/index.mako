<%inherit file="../layouts/application.mako"/>

<%
    from pylons import url
    from routes.util import url_for
    from anygit import models
%>

<p> Welcome to <b>anygit</b>, indexing the world's Git repositories
one at a time.  Currently
<b>${models.Repository.count(been_indexed=True)}</b> repos have been
indexed and counting.  We also have thus far indexed
<b>${models.Blob.count(complete=True)}</b> blobs,
<b>${models.Tree.count(complete=True)}</b> trees, and
<b>${models.Commit.count(complete=True)}</b> commits.</p>

<h2> Request indexing </h2>

<p>
Would you like your repository to be indexed next?

<%self:form url="${url_for(controller='index', action='do_request')}">
<p> <label for="url">Git URL:</label>
${webhelpers.html.tags.text('url')}
${webhelpers.html.tags.submit('submit', 'Index me please')} </p>
</%self:form> </p>

<h2> Query </h2>

<p>
You can query for any Git object by going to <b>http://gdb.mit.edu/anygit/q/$sha1prefix</b>
</p>

<p>
Alternatively, just enter your SHA1 prefix in the textfield:
</p>

<p>
<form type="GET" action="${url_for(controller='query', action='query_with_string')}" />
<input type="text" name="query" value="" />
<input type="submit" value="Query" />
</form>
</p>
