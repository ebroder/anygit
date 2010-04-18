<%inherit file="../layouts/application.mako"/>

<ul>
% for object in c.objects:

% if object.type == 'commit':
<li> Commit ${object.id} comes from repositories: ${', '.join([r.url for r in object.repositories])} </li>
% elif object.type == 'blob':
<li> Blob ${object.id} comes from commits: ${', '.join([r.id for r in object.commits])} </li>
% elif object.type == 'tree':
<li> Tree ${object.id} comes from commits: ${', '.join([r.id for r in object.commits])} </li>
% elif object.type == 'tag':
<li> Tree ${object.id} points to commit: ${object.commit.id} </li>
% endif

% endfor
</ul>

% if c.count:
<p> Showing matching <b>1</b>-<b>${c.count}</b>.  (Sorry, we haven't
gotten around to implementing paging yet, so you'll have to try a more
specific query if you want more results.) </p>
% else:
<p> No results </p>
% endif