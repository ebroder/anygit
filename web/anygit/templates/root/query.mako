<%inherit file="layouts/application.mako"/>

% for object in objects:
${object.__class__.__name__}: ${object.sha1}
% endfor
