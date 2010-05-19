<%inherit file="../layouts/application.mako"/>

<%
  import pylons
  from routes.util import url_for
%>

% if c.objects.count() == 0:
<p> Sorry, no objects were found with that SHA1 as a prefix. </p>

% elif c.objects.count() == 1:    
<% object = c.objects.next() %>
  % if object.type == 'commit':
    <% parents = object.repositories %>
    <p> Commit ${self.link_to_object(object)} appears in the following
    ${h.pluralize(parents.count(), 'repository', 'repositories')}: </p>

      <ul>
      % for parent in parents:
      <li> ${self.link_to_view(parent, object)} </li>
      % endfor
      </ul>

  % elif object.type == 'blob':
    <% parents = object.repositories %>
    <p> Blob ${self.link_to_object(object)} has been found in the following
    ${h.pluralize(parents.count(), 'repository', 'repositories')}: </p>
    <ul>
    % for parent in parents:
      <li> ${self.link_to_view(parent, object)} </li>
    % endfor
    </ul>
    </p>

    <p> Additionally, this blob comes from the following 
    ${h.pluralize(len(object.commit_ids), 'commit')}: </p>
    <ul>
    % for commit in object.commits:
      <li> ${self.link_to_object(commit)} </li>
    % endfor
    </ul>
    </p>

    <p> Finally, this blob comes from the following 
    ${h.pluralize(len(object.parent_ids), 'tree')}: </p>
    <ul>
    % for tree in object.parents:
      <li> ${self.link_to_object(tree)} </li>
    % endfor
    </ul>
    </p>

  % elif object.type == 'tree':
    <% parents = object.repositories %>
    <p> Tree ${self.link_to_object(object)} has been found in the following
    ${h.pluralize(parents.count(), 'repository', 'repositories')}: </p>
    <ul>
    % for parent in parents:
      <li> ${self.link_to_view(parent, object)} </li>
    % endfor
    </ul>
    </p>

    <p> Additionally, this tree comes from the following 
    ${h.pluralize(len(object.commit_ids), 'commit')}: </p>
    <ul>
    % for commit in object.commits:
      <li> ${self.link_to_object(commit)} </li>
    % endfor
    </ul>
    </p>

  % elif object.type == 'tag':
    <% parents = object.repositories %>
    <p> Tag ${self.link_to_object(object)} has been found in the following
    ${h.pluralize(parents.count(), 'repository', 'repositories')}: </p>
    <ul>
    % for parent in parents:
      <li> ${self.link_to_view(parent, object)} </li>
    % endfor
    </ul>
    </p>
  % else:
    <% raise ValueError('Unrecognized type for %s' % object) %>
  % endif

% else:

% if not c.out_of_range:
  <p> You queried for git objects with prefix <b>${c.queried_id}</b>.  Showing results
   <b>${c.start}-${c.end}</b> of <b>${c.count}</b>.

% for i in range(c.page - 4, c.page + 3):
  % if i < 0:
    <% continue %>
  % elif c.limit * i > c.count:
    <% continue %>
  % elif c.page == i + 1:
    <b>${i + 1}</b>
  % else:
  <a href="${url_for(controller='query', action='query',
  id=c.queried_id, page=i + 1)}">${i + 1}</a> 
  % endif
% endfor
</p>
% else:
  <p> You queried for git objects with prefix <b>${c.queried_id}</b>.  There were
   <b>${c.count}</b> results.  Requested start (<b>${c.start}</b>) out of range.
% endif


% for object in c.objects:
% if object.type == 'commit':
<li> Commit ${self.link_to_object(object)} comes from ${h.pluralize(len(object.repository_ids), 'repository', 'repositories')}. </li>
% elif object.type == 'blob':
<li>
  Blob ${self.link_to_object(object)} comes from
  ${h.pluralize(len(object.parent_ids), 'tree')},
  ${h.pluralize(len(object.commit_ids), 'commit')}, and
  ${h.pluralize(len(object.repository_ids), 'repository', 'repositories')}.
</li>
% elif object.type == 'tree':
<li>
  Tree ${self.link_to_object(object)} comes from 
  ${h.pluralize(len(object.commit_ids), 'commit')}, and
  ${h.pluralize(len(object.repository_ids), 'repository', 'repositories')}.
</li>
% elif object.type == 'tag':
<li>
  Tag ${self.link_to_object(object)} comes from 
  ${h.pluralize(len(object.repository_ids), 'repository', 'repositories')}.
</li>
% endif
% endfor

% endif

<p>
<form type="GET" action="${url_for(controller='query', action='query_with_string')}" />
<input type="text" name="query" value="${c.queried_id}" />
<input type="submit" value="Update query" />
</form>
</p>
