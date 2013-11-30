<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"
        import="io.teknek.datalayer.*,java.util.*,org.apache.zookeeper.*,io.teknek.plan.*"
        %>
<%! Watcher dummy = new Watcher(){ public void process(WatchedEvent event) { } }; %>
<% ZooKeeper zk = new ZooKeeper((String) session.getAttribute("zkconnect"), 100, dummy); %>
<% 
if (request.getParameter("plan") != null){
  Plan p = WorkerDao.deserializePlan(request.getParameter("plan").getBytes());
  WorkerDao.createOrUpdatePlan(p, zk);
  request.setAttribute("message", "plan "+p.getName()+" created");
}
zk.close();
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Insert title here</title>
</head>
<body>
<% if (request.getAttribute("message") != null){ %>
	<%= request.getAttribute("message") %>
<% } %>
<form>
<textarea name="plan" rows="20" cols="40"></textarea>
<input type="submit">
</form>
</body>
</html>
