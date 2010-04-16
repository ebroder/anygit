<%inherit file="../layouts/application.mako"/>

<ul>
% for object in c.objects:
% if object.type == 'commit':
<li> Commit ${object.id} comes from repositories: ${', '.join([r.url for r in object.repositories])} </li>
% elif object.type == 'blob':
<li> Blob ${object.id} comes from commits: ${', '.join([r.name for r in object.commits])} and repositories: ${', '.join([r.url for r in object.repositories])} </li>
% endif
% endfor
</ul>
