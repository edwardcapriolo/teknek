<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"
    import="io.teknek.datalayer.*,java.util.*,org.apache.zookeeper.*"
    %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<%! 
Watcher dummy = new Watcher(){
  public void process(WatchedEvent event) {
       
  }
};
%>
<% 
ZooKeeper zk = new ZooKeeper((String) session.getAttribute("zkconnect"), 100, dummy);
%>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Insert title here</title>
</head>
<body>
	<table border="1" width="100%">
		<tr>
			<!-- left -->
			<td>
			<% List<String> plans = WorkerDao.finalAllPlanNames(zk); %>
			<table border=1>
				<% for (String plan: plans) { %>
				<tr>
					<td><%=plan%></td>
				</tr>
				<% } %>
			</table>
			<a href="create-plan.jsp">Create plan</a>
			</td>
			<!-- center -->
			<td ></td>
		<tr>
	</table>
</body>
</html>
<% zk.close(); %>