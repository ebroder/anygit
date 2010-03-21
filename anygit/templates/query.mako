<%inherit file="layouts/application.mako"/>

<ul>
% for object in c.objects:
<% 
t = object.type
print t
%>
% if t == 'Commit':
<li> Commit ${object.sha1} comes from repositories: ${', '.join([r.url for r in object.repositories])} </li>
% elif t == 'Blob':
<li> Blob ${object.sha1} comes from commits: ${', '.join([r.sha1 for r in object.commits])} and repositories: ${', '.join([r.url for r in object.repositories])} </li>
% endif
% endfor
</ul>
