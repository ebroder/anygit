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

<p>
<%self:form url="${url_for(controller='query', action='query_with_string')}">
${webhelpers.html.tags.text('query', c.queried_id)}
${webhelpers.html.tags.select('limit', c.limit, sorted(set([4, 6, 10, 16, 25, 40, c.limit])))}
${webhelpers.html.tags.submit('submit', 'Update')}
</%self:form>
</p>

% if c.objects.count() == 0:
<p> Sorry, no objects were found with <tt>${c.queried_id}</tt> as a prefix. </p>

% elif c.objects.count() == 1:
<% object = c.objects.next() %>
% if object.dirty:
<p><i>Note that this object is currently being indexed; its state might appear broken.</i></p>
% endif

  % if object.type == 'commit':
    <% repos = object.limited_repositories(100) %>
    <p> Commit <tt>${self.show_object(object)}</tt> appears in the following
    ${h.pluralize(repos.count(), 'repository', 'repositories', when='plural')}: </p>
      <ul class="results">
      % for repo in repos:
      <li> ${self.link_to_view(repo, object)} </li>
      % endfor
      % if repos.count() > 100:
      <li> And so on and so forth </li>
      % endif
      </ul>

  % elif object.type == 'blob':
    <% repos = object.limited_repositories(100) %>
    <p> Blob <tt>${self.show_object(object)}</tt> has been found in the following
    ${h.pluralize(repos.count(), 'repository', 'repositories', when='plural')}: </p>
    <ul class="results">
    % for repo in repos:
      <li> ${self.link_to_view(repo, object)} </li>
    % endfor
      % if repos.count() > 100:
      <li> And so on and so forth </li>
      % endif
    </ul>
    </p>

    <% parent_ids = object.limited_parent_ids(100) %>
    % if parent_ids.count():
    <p> Also, this blob comes from the following 
    ${h.pluralize(parent_ids.count(), 'tree', when='plural')}. 

    <% names = object.limited_names(100) %>
    % if names.count() == 0:
      <% assert object.dirty %>
      We haven't gotten around to recording its file name just yet, but hopefully will
      soon.  If you're feeling especially inquisitive, feel free to email
      <a href="mailto:anygit@mit.edu">anygit@mit.edu</a> and ask what's up with
      <tt>${object.id}</tt>.
    % elif names.count() == 1:
       Its file name is <tt>${names.next()}</tt>
    % else:
       It has had several file names over time, namely 
          ${h.liststyled(names, ', ', '<tt>', '</tt>') | n}:
    % endif
    </p>
    % else:
    <p> It is not contained in any tree. </p>
    % endif

    <ul class="results">
    % for tree_id in parent_ids:
      <li><tt>${self.link_to_object(tree_id)}</tt></li>
    % endfor
      % if parent_ids.count() > 100:
      <li> And so on and so forth </li>
      % endif
    </ul>
    </p>

  % elif object.type == 'tree':
    <% repos = object.limited_repositories(100) %>
    <p> Tree <tt>${self.show_object(object)}</tt> has been found in the following
    ${h.pluralize(repos.count(), 'repository', 'repositories', when='plural')}: </p>
    <ul class="results">
    % for repo in repos:
      <li> ${self.link_to_view(repo, object)} </li>
    % endfor
      % if repos.count() > 100:
      <li> And so on and so forth </li>
      % endif
    </ul>
    </p>


    % if object.commit_ids.count():
      <p> Additionally, this tree comes from the following  
      ${h.pluralize(object.commit_ids.count(), 'commit', when='plural')}: </p>
      <ul class="results">
      <% commit_ids = object.limited_commit_ids(100) %>
      % for commit_id in commit_ids:
         <li> <tt>${self.link_to_object(commit_id)}</tt> </li>
      % endfor
      % if commit_ids.count() > 100:
      <li> And so on and so forth </li>
      % endif
      </ul>
      </p>
    % else:
      <p> It is not the tree of any commit. </p>
    % endif

    <% parent_ids = object.limited_parent_ids_with_names(100) %>
    % if parent_ids.count():
    <p> Finally, it is a subtree of the following 
    ${h.pluralize(parent_ids.count(), 'tree', when='plural')}.

    <% names = object.limited_names(100) %>
    % if names.count() == 0:
      <% assert object.dirty %>
      We haven't gotten around to recording its directory name just yet, but hopefully will
      soon.  If you're feeling especially inquisitive, feel free to email
      <a href="mailto:anygit@mit.edu">anygit@mit.edu</a> and ask what's up with
      <tt>${object.id}</tt>.
    % elif names.count() == 1:
       Its name as a directory is <tt>${names.next()}</tt>
    % else:
       It has been known by the following names:
          ${h.liststyled(names, ', ', '<tt>', '</tt>') | n}: </p>
    % endif
    <ul class="results">
    % for tree_id, name in parent_ids:
      <li> <tt>${self.link_to_object(tree_id)}</tt>
      % if names.count() > 1:
        (<tt>${name}</tt>)
      % endif
      </li>
    % endfor
    % if parent_ids.count() > 100:
    <li> And so on and so forth </li>
    % endif
    </ul>
    </p>
    % else:
    <p> It is not a subtree of any tree. </p>
    % endif

  % elif object.type == 'tag':
    <% repos = object.limited_repositories(100) %>
    <p> Tag <tt>${self.show_object(object)}</tt> has been found in the following
    ${h.pluralize(repos.count(), 'repository', 'repositories', when='plural')}: </p>
    <ul class="results">
    % for repo in repos:
      <li> ${self.link_to_view(repo, object)} </li>
    % endfor
    % if repos.count() > 100:
    <li> And so on and so forth </li>
    % endif
    </ul>
    </p>

  % else:
    <% raise ValueError('Unrecognized type for %s' % object) %>
  % endif

% else:

<p> You queried for git objects with prefix <tt>${c.queried_id}</tt>. </p>

<ul class="results">
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
</ul>

% if not c.out_of_range:
  <p> Showing results
   <b>${c.start}-${c.end}</b> of <b>${c.count}</b>.
  </p>
  Pages: 
  % if c.page - 3 == 1:
    <a href="${url_for(controller='query', action='query', limit=c.limit,
    id=c.queried_id, page=1)}">${1}</a>
  % elif c.page - 3 > 1:
    <a href="${url_for(controller='query', action='query', limit=c.limit,
    id=c.queried_id, page=1)}">${1}</a> ... 
  % endif
  % for i in range(c.page - 3, c.page + 2):
    % if i < 0:
      <% continue %>
    % elif c.limit * i >= c.count:
      <% continue %>
    % elif c.page == i + 1:
      <b>${i + 1}</b>
    % else:
    <a href="${url_for(controller='query', action='query', limit=c.limit,
    id=c.queried_id, page=i + 1)}">${i + 1}</a> 
    % endif
  % endfor
  % if c.count % c.limit == 0:
    % if c.page + 3 == ( c.count / c.limit ):
      <a href="${url_for(controller='query', action='query', limit=c.limit,
      id=c.queried_id, page=(c.count / c.limit))}">${(c.count / c.limit)}</a>
    % elif c.page + 3 < ( c.count / c.limit ):
      ... <a href="${url_for(controller='query', action='query', limit=c.limit,
      id=c.queried_id, page=(c.count / c.limit))}">${(c.count / c.limit)}</a>
    % endif
  % else:
    % if c.page + 3 == ( c.count / c.limit ) + 1:
      <a href="${url_for(controller='query', action='query', limit=c.limit,
      id=c.queried_id, page=(c.count / c.limit) + 1)}">${(c.count / c.limit) + 1}</a>
    % elif c.page + 3 < ( c.count / c.limit ) + 1:
      ... <a href="${url_for(controller='query', action='query', limit=c.limit,
      id=c.queried_id, page=(c.count / c.limit) + 1)}">${(c.count / c.limit) + 1}</a>
    % endif
  % endif
% else:
  <p> There were
   <b>${c.count}</b> results.  Requested start (<b>${c.start}</b>) out of range.
  </p>
% endif

% endif
