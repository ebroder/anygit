<%page cached="True"/>

<%inherit file="../layouts/application.mako"/>

<%
    from pylons import url
    from routes.util import url_for
    from anygit import models
    aggregate = models.Aggregate.get()
%>

<p>Welcome to <b>anygit</b>, indexing the world's Git repositories
one at a time. Currently
<b>${aggregate.indexed_repository_count}</b> repos have been
indexed and counting. We also have thus far indexed
<b>${aggregate.blob_count}</b> blobs,
<b>${aggregate.tree_count}</b> trees,
<b>${aggregate.commit_count}</b> commits, and
<b>${aggregate.tag_count}</b> tags.</p>

<h2>Request indexing</h2>

<p>
Would you like your repository to be added to the index?

<%self:form url="${url_for(controller='index', action='do_request')}">
<p> <label for="url">Git URL:</label>
${webhelpers.html.tags.text('url')}
${webhelpers.html.tags.submit('submit', 'Index me please')} </p>
</%self:form> </p>

<h2>Query</h2>

<p>
You can query for any Git object by going to <b>http://anyg.it/q/$sha1prefix</b>
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

<p> The most populous repositories are: </p>
<ol>
% for r in models.Repository.get_by_highest_count(10):
<li> ${r.url} with ${r.count} git objects </li>
% endfor
</ol>