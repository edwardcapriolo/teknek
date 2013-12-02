<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"
    import="io.teknek.datalayer.*,io.teknek.sol.*,java.util.*,org.apache.zookeeper.*,org.codehaus.jackson.map.*"
    %>
<%! Map<Integer,Sol> sols = new HashMap<Integer,Sol>(); %>
<% 
System.out.println(request.getParameter("consoleId")); 
System.out.println(request.getParameter("command")); 

int consoleId = Integer.parseInt(request.getParameter("consoleId"));
String command = request.getParameter("command");
Sol sol = sols.get(consoleId);
if (sol == null) {
  sol = new Sol();
  sols.put(consoleId, sol);
} 
SolReturn sr = sol.send(command);
ObjectMapper om = new ObjectMapper();

%><%= om.writeValueAsString(sr) %>