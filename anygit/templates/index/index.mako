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

<p>
You can <a href="request">request</a> that your Git repo be indeed.
</p>

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