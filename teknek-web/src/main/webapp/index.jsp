<html>
<body>
<h2>Hello Teknek!</h2>
<a href="operator-lab/index.jsp">Operator Lab</a>
<a href="plan-manager/index.jsp">Plan Manager</a>
<%
  if (request.getParameter("zkconnect") != null) {
    session.setAttribute("zkconnect", request.getParameter("zkconnect"));
  }
%>
<form>
	ZookeeperHost: <input type="text" name="zkconnect" 
	value="<% if (session.getAttribute("zkconnect")!=null){ out.print( session.getAttribute("zkconnect")); } %>"><br>
	<input type=submit name="send">
</form>
</body>
</html>
