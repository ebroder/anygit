<%inherit file="../layouts/application.mako"/>

<%
  import pylons
  from routes.util import url_for
%>

<%def name="display_contents(obj)">
% for repo in obj.repositories:
  % if not repo.linkable(obj) and repo.displayable(obj):
    ${repo.display_object(obj)}
  % endif
% endfor
</%def>


% if c.objects.count() == 0:
<p> Sorry, no objects were found with <tt>${c.queried_id}</tt> as a prefix. </p>

% elif c.objects.count() == 1:
<% object = c.objects.next() %>
% if object.dirty:
<p><i>Note that this object is currently being indexed; its state might appear broken.</i></p>
% endif

  % if object.type == 'commit':
    <% repos = object.repositories %>
    <p> Commit <tt>${self.link_to_object(object)}</tt> appears in the following
    ${h.pluralize(repos.count(), 'repository', 'repositories', when='plural')}: </p>
      <ul>
      % for repo in repos:
      <li> ${self.link_to_view(repo, object)} </li>
      % endfor
      </ul>

  % elif object.type == 'blob':
    <% repos = object.repositories %>
    <p> Blob <tt>${self.link_to_object(object)}</tt> has been found in the following
    ${h.pluralize(repos.count(), 'repository', 'repositories', when='plural')}: </p>
    <ul>
    % for repo in repos:
      <li> ${self.link_to_view(repo, object)} </li>
    % endfor
    </ul>
    </p>

    <p> Also, this blob comes from the following 
    ${h.pluralize(object.parent_ids.count(), 'tree', when='plural')}. It has ${h.pluralize(object.names.count(), 'file', when='never')} associated with this blob are
    ${h.liststyled(object.names, ', ', '<tt>', '</tt>') | n}: </p>
    <ul>
    % for tree_id in object.parent_ids:
      <li><tt>${self.link_to_object(tree_id)}</tt></li>
    % endfor
    </ul>
    </p>

  % elif object.type == 'tree':
    <% repos = object.repositories %>
    <p> Tree <tt>${self.link_to_object(object)}</tt> has been found in the following
    ${h.pluralize(repos.count(), 'repository', 'repositories', when='plural')}: </p>
    <ul>
    % for repo in repos:
      <li> ${self.link_to_view(repo, object)} </li>
    % endfor
    </ul>
    </p>


    % if object.commit_ids.count():
      <p> Additionally, this tree comes from the following  
      ${h.pluralize(object.commit_ids.count(), 'commit', when='plural')}: </p>
      <ul>
      % for commit_id in object.commit_ids:
         <li> <tt>${self.link_to_object(commit_id)}</tt> </li>
      % endfor
      </ul>
      </p>
    % else:
      <p> It is not the tree of any commit. </p>
    % endif

    % if object.parent_ids.count():
    <p> Finally, it is a subtree of the following 
    ${h.pluralize(object.parent_ids.count(), 'tree', when='plural')}.  Its directory name is
    ${h.liststyled(object.names, ', ', '<tt>', '</tt>') | n}: </p>
    <ul>
    % for tree_id in object.parent_ids:
      <li> <tt>${self.link_to_object(tree_id)}</tt> </li>
    % endfor
    </ul>
    </p>
    % else:
    <p> It is not a subtree of any tree. </p>
    % endif

  % elif object.type == 'tag':
    <% repos = object.repositories %>
    <p> Tag <tt>${self.link_to_object(object)}</tt> has been found in the following
    ${h.pluralize(repos.count(), 'repository', 'repositories', when='plural')}: </p>
    <ul>
    % for repo in repos:
      <li> ${self.link_to_view(repo, object)} </li>
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
  Pages: 
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
<li> Commit <tt>${self.link_to_object(object)}</tt> comes from ${h.pluralize(object.repository_ids.count(), 'repository', 'repositories')}. </li>
% elif object.type == 'blob':
<li>
  Blob <tt>${self.link_to_object(object)}</tt> comes from
  ${h.pluralize(object.parent_ids.count(), 'tree')} and
  ${h.pluralize(object.repository_ids.count(), 'repository', 'repositories')}.
</li>
% elif object.type == 'tree':
<li>
  Tree <tt>${self.link_to_object(object)}</tt> comes from 
  % if object.commit_ids and object.parent_ids:
    ${h.pluralize(object.commit_ids.count(), 'commit')},
    ${h.pluralize(object.parent_ids.count(), 'parent tree')}, and
    ${h.pluralize(object.repository_ids.count(), 'repository', 'repositories')}.
  % elif object.commit_ids:
    ${h.pluralize(object.commit_ids.count(), 'commit')} and
    ${h.pluralize(object.repository_ids.count(), 'repository', 'repositories')}.
  % elif object.parent_ids:
    ${h.pluralize(object.parent_ids.count(), 'parent tree')} and
    ${h.pluralize(object.repository_ids.count(), 'repository', 'repositories')}.
  % else:
    ${h.pluralize(object.repository_ids.count(), 'repository', 'repositories')}.
  % endif
</li>
% elif object.type == 'tag':
<li>
  Tag <tt>${self.link_to_object(object)}</tt> comes from 
  ${h.pluralize(object.repository_ids.count(), 'repository', 'repositories')}.
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
