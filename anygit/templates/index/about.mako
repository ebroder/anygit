<%page cached="True"/>

<%inherit file="../layouts/application.mako"/>
<%
       from pylons import url
%>

<h2> What is anygit? </h2>

<p> <b>anygit</b> is a project of the <a
href="http://sipb.mit.edu">Student Information Processing Board</a>,
MIT's student computing group. </p>

<h2> What does it do? </h2>

<p> Think of <b>anygit</b> as a search engine for <a
href="http://git-scm.com/">git</a> repositories.  We take the git
object model and turn it on its head. </p>

<p> Here's how it works: you give us a SHA1 (or a SHA1 prefix) of a
git object.  <b>anygit</b> will then consult our
painstakingly-compiled index to provide you with information about
in which repositories and other git objects the requested object appears. </p>

In particular,

<ul>
<li>
For any requested object, <b>anygit</b> provides the list of
repositories that object appears in, as well as any tags that may
point to that object.
</li>
<li>
For a blob, <b>anygit</b> will provide the trees that the
blob appears in, as well as its filename in that tree.
</li>
<li>
For a tree, we will spit back the set of supertrees of that tree (as
well as any associated filenames) and any commits that point to that
tree.
</li>
</ul>

<h2> Where did this marvelous invention come from? </h2>

<p> <b>anygit</b> was the brainchild of <a
href="http://ebroder.net">Evan Broder</a>.  In his extensive usage of
git as a developer, Evan often found himself wondering how far his
commits were traveling.  He decided to put into place a project to
track this.  So one fateful night, he gathered around him a group of
MIT students, and they all swore they would not rest until his vision
became a reality. </p>

<p> The chief developer for the project is <a
href="http://gregbrockman.com">Greg Brockman</a>.  Other contributors
include Alan Huang, <a
href="http://www.comclub.org/~quentins/about">Quentin Smith</a>, <a
href="http://web.mit.edu/davidben/www/">David Benjamin</a>, and <a
href="http://web.mit.edu/price">Greg Price</a>. </p>

<h2> What are the intended use cases? </h2>

<p> Dunno.  Email us at <a
href="mailto:anygit@mit.edu">anygit@mit.edu</a> if you have any great
ideas. </p>

<h2> Where can I get the code for anygit? </h2>

<p> The code for anygit is freely available (under the AGPLv3+ license) on
<a href="http://github.com/ebroder/anygit">GitHub</a>. </p>

<h2> I have no idea what's going on here, how can I learn more about git? </h2>

<p> There are many excellent resources for git available for free on the internets: </p>

<ul>
<li><a href="http://blog.nelhage.com/2010/01/git-in-pictures/">Git in pictures</a></li>
<li><a href="http://eagain.net/articles/git-for-computer-scientists/">Git for computer scientists</a></li>
<li> <a href="http://marklodato.github.com/visual-git-guide/">Visual git guide</a> </li>
</ul>
