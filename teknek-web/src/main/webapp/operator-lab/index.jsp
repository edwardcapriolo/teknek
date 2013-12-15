<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"
    import="java.util.*,io.teknek.model.*, io.teknek.plan.*, io.teknek.driver.*,io.teknek.collector.*"
    %>
    
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Insert title here</title>
</head>
<%
List<ITuple> input = new ArrayList<ITuple>();
input.add( new Tuple().withField("name", "bob").withField("age", 40));
input.add( new Tuple().withField("name", "tom").withField("age", 30));
input.add( new Tuple().withField("name", "pete").withField("age", 03));
input.add( new Tuple().withField("name", "shelly").withField("age", 10));
input.add( new Tuple().withField("name", "lincoln").withField("age", 19));
%>
<body>
	<table border="1" width="100%">
		<tr>
		<td>
			Source Data<br>
			<table border="1">
				<% for (ITuple t: input) { %>
				<tr><td><%=t%></td></tr>
				<% } %>
			</table>
		</td>
		<td>
			<form method=POST>
				Spec: <select name="spec">
					<option value="groovyclosure">Groovy Closure</option>
					<option value="groovy">Groovy Class</option>
				</select>
				<br> Script:
				<textarea rows=15 cols=40 name="script"><%if (request.getParameter("script")!=null) { out.print(request.getParameter("script")); }%></textarea>
				<br> Class:
				<input type="text" name="class"
				value="<%if (request.getParameter("class")!=null) { out.print(request.getParameter("class")); }%>"
				
				>
				<br>
				<input type="submit">
			</form>
		</td>
		<td>
			<%
			  Operator operator = null;
			  if (request.getParameter("script") != null){
			    OperatorDesc o = new OperatorDesc();
			    o.setSpec(request.getParameter("spec"));
			    o.setTheClass(request.getParameter("class"));
			    //o.setScript("{ tuple, collector ->  collector.emit(tuple) }");
			    o.setScript(request.getParameter("script"));
			    operator = DriverFactory.buildOperator(o);
			    operator.setCollector(new Collector());
			    
			    for (ITuple t: input) {
			      operator.handleTuple(t);
			    }
			  }
			%>
			Result Data<br>
			<% if (operator != null) { %>
				<table border="1">
					<% Collector c = (Collector) operator.getCollector();
					int size = c.size();
					for (int i=0; i< size ;i++){ %>
					<tr><td><%= c.take() %></td></tr>
					<% } %>
				</table>
			<% } %>
		
		
		</td>
		</tr>
	 </table>
</body>
</html>