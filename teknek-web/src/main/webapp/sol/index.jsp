<%!
int x=0;
%>
<html>
  <head>
    <title>TekNek Streaming Operator Language!</title>
    <link rel="stylesheet" type="text/css" href="webconsole.css" />
  </head>
  <body>

    <form action="exec.jsp" method="get" id="console-form">
      <div
        class="console"
        id="result">
        Welcome TekNek SOL 
        <br />
        teknek&gt;
      </div>
      <input type=hidden name="consoleId" value="<%=x++%>">
      <input
        class="console"
        name="command"
        id="command"
        type="text" />
    </form>


    <script type="text/javascript" src="yahoo-min.js"></script>
    <script type="text/javascript" src="event-min.js"></script>
    <script type="text/javascript" src="connection-min.js"></script>

    <script type="text/javascript">

    var WebConsole = {};

    WebConsole.printResult = function(result_string, result_prompt)
    {
      var result_div = document.getElementById('result');
      var result_array = result_string.split('\n');

      var new_command = document.getElementById('command').value;
      result_div.appendChild(document.createTextNode(new_command));
      result_div.appendChild(document.createElement('br'));

      var result_wrap, line_index, line;

      for (line_index in result_array) {
        result_wrap = document.createElement('pre');
        line = document.createTextNode(result_array[line_index]);
        result_wrap.appendChild(line);
        result_div.appendChild(result_wrap);
        result_div.appendChild(document.createElement('br'));

      }
      //result_div.appendChild(document.createTextNode(':-> '));
      result_div.appendChild(document.createTextNode(result_prompt));

      result_div.scrollTop = result_div.scrollHeight;
      document.getElementById('command').value = '';
    };

    WebConsole.keyEvent = function(event)
    {

      var the_url, the_shell_command;
      switch(event.keyCode){
        case 13:
          the_shell_command = document.getElementById('command').value;
          if (the_shell_command) {
            this.commands_history[this.commands_history.length] = the_shell_command;
            this.history_pointer = this.commands_history.length;

            // YUI's AJAX
            YAHOO.util.Connect.setForm(document.forms[0]);
            YAHOO.util.Connect.asyncRequest(
                'GET',
                'exec.jsp',
                {
                  success: function(xhr){
                	//{"prompt":"teknek> ","message":"Only valid commands are CREATE"}
                	obj = JSON.parse(xhr.responseText);
                	WebConsole.printResult(obj.message, obj.prompt);
                    //WebConsole.printResult(xhr.responseText)
		      		//WebConsole.printResult("yea man it worked");
                  }
                }
            );

          }
          break;
        case 38: // this is the arrow up
          if (this.history_pointer > 0) {
            this.history_pointer--;
            document.getElementById('command').value = this.commands_history[this.history_pointer];
          }
          break;
        case 40: // this is the arrow down
          if (this.history_pointer < this.commands_history.length - 1 ) {
            this.history_pointer++;
            document.getElementById('command').value = this.commands_history[this.history_pointer];
          }
          break;
        default:
          break;
      }
    };

    WebConsole.commands_history = [];
    WebConsole.history_pointer = 0;


    document.getElementById('console-form').onsubmit = function(){
        return false;
    };
    document.getElementById('command').onkeyup = function(e){
        if (!e && window.event) {
            e = window.event;
        }
        WebConsole.keyEvent(e);
    };

    </script>
  </body>
</html>
