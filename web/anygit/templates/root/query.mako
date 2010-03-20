<%inherit file="layouts/application.mako"/>

<ul>
% for object in objects:
<li> ${object.type} ${object.sha1} is from repository ${object.sha1} </li>
% endfor
</ul>
