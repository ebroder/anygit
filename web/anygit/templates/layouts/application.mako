<html>
    <body>
        <div class="header">
            ${self.header()}
        </div>

        ${self.body()}

        <div class="footer">
            ${self.footer()}
        </div>
    </body>
</html>

<%def name="header()">
    Hello world
</%def>

<%def name="footer()">
    Brought to you by the anyg.it team
</%def>
