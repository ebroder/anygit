<%page cached="True"/>

<%inherit file="../layouts/application.mako"/>

<%
    from pylons import url
    from routes.util import url_for
    from anygit import models
    aggregate = models.Aggregate.get()
%>

<script type="text/javascript">
	function setfocus(elt) { document.getElementById(elt).focus(); }
	
	function expand() {
		urlbox = document.getElementById('url');
		text = urlbox.value;
		parent = urlbox.parentNode;
		parent.removeChild(urlbox);
		if(urlbox.nodeName.toLowerCase() == 'input') {
			urlbox = document.createElement('textarea');
			urlbox.setAttribute('cols', '20');
			urlbox.setAttribute('rows', '5');
			document.getElementById('expand').innerHTML = '&#9650;';
		} else {
			urlbox = document.createElement('input');
			urlbox.setAttribute('type', 'text');
			urlbox.setAttribute('size', '20');
			document.getElementById('expand').innerHTML = '&#9660;';
		}
		urlbox.setAttribute('id', 'url');
		urlbox.setAttribute('name', 'url');
		urlbox.setAttribute('title', 'URL of the repository');
		urlbox.value = text;
		parent.insertBefore(urlbox, document.getElementById('submit').previousSibling);
	}
</script>

<table><tr><td>

<div class="box" id="current">
	<div class="info"><h2>Currently indexed:</h2></div>
	<div id="repo"><b>${aggregate.indexed_repository_count} </b>
		${h.pluralize(aggregate.indexed_repository_count, 'repo', when='never')}
		<img src="${url('/static/git-repo.png')}" onclick="setfocus('url');"></div>
	<div id="blob"><b>${aggregate.blob_count} </b>
		${h.pluralize(aggregate.blob_count, 'blob', when='never')}
		<img src="${url('/static/git-blob.png')}" onclick="setfocus('query');"></div>
	<div id="tree"><b>${aggregate.tree_count} </b>
		${h.pluralize(aggregate.tree_count, 'tree', when='never')}
		<img src="${url('/static/git-tree.png')}" onclick="setfocus('query');"></div>
	<div id="commit"><b>${aggregate.commit_count} </b>
		${h.pluralize(aggregate.commit_count, 'commit', when='never')}
		<img src="${url('/static/git-commit.png')}" onclick="setfocus('query');"></div>
	<div id="tag"><b>${aggregate.tag_count} </b>
		${h.pluralize(aggregate.tag_count, 'tag', when='never')}
		<img src="${url('/static/git-tag.png')}" onclick="setfocus('query');"></div>
</div>

</td><td>

<div class="box" id="request">
	<div class="info"><h2>Request indexing:</h2></div>
	<div id="add">
		<p>Would you like your repository to be added to the index? Enter the Git URL here.</p>
		<%self:form url="${url_for(controller='index', action='do_request')}">
		<a id="expand" onclick="expand();">▼</a>
		${webhelpers.html.tags.text('url', title='URL of the repository')}
		${webhelpers.html.tags.submit('submit', 'Index')}
		</%self:form>
	</div>
</div>

<div class="box" id="search">
	<div class="info"><h2>Object lookup:</h2></div>
	<div id="sha">
		<p>You can query for any Git object by going to <b>http://anyg.it/q/$sha1prefix</b>.</p>
		<p>Alternatively, just enter your SHA1 prefix in the textfield:</p>
		<%self:form url="${url_for(controller='query', action='query_with_string')}">
		${webhelpers.html.tags.text('query', title='SHA-1 hash to search for')}
		${webhelpers.html.tags.submit('qsubmit', 'Query')}
		</%self:form>
	</div>
</div>

</td><td>

<div class="box" id="stats">
	<div class="info"><h2>Largest repositories:</h2></div>
	<div id="largest">
		<ol>
		% for r in models.Repository.get_by_highest_count(5):
		<li> <b>${r.url}</b><br />with <b>${r.count}</b> git objects </li>
		% endfor
		</ol>
	</div>
</div>

</td></tr></table>

<br />
