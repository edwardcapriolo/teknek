<html>
<body>
<h2>Hello Teknek!</h2>

Configure plans from the <a href="sol/index.jsp">SOL shell</a> <br>
<%
  if (request.getParameter("zkconnect") != null) {
    session.setAttribute("zkconnect", request.getParameter("zkconnect"));
  }
%>
<br>
<table border="1">
<tr>
	<form>
		<td>ZookeeperHost</td> 
		<td><input type="text" name="zkconnect" 
		value="<% if (session.getAttribute("zkconnect")!=null){ out.print( session.getAttribute("zkconnect")); } %>"></td>
		<td><input type=submit name="send"></td>
	</form>
</tr>
</table>
<p>
experimental:<br>
<a href="upload/upload.jsp">Upload resources</a><br>
<a href="operator-lab/index.jsp">Operator Lab</a><br>
<a href="plan-manager/index.jsp">Plan Manager</a><br>
</p>
</body>
</html>
