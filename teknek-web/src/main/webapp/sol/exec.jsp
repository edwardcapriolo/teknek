<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"
    import="io.teknek.datalayer.*,io.teknek.sol.*,java.util.*,org.apache.zookeeper.*,org.codehaus.jackson.map.*"
    %>
<%! Map<Integer,Sol> sols = new HashMap<Integer,Sol>(); %>
<%! Watcher dummy = new Watcher(){ public void process(WatchedEvent event) { } }; %>
<% 

int consoleId = Integer.parseInt(request.getParameter("consoleId"));
String command = request.getParameter("command");
Sol sol = sols.get(consoleId);
if (sol == null) {
  sol = new Sol();
  sols.put(consoleId, sol);
  ZooKeeper zk = new ZooKeeper((String) session.getAttribute("zkconnect"), 100, dummy);
  sol.setZookeeper(zk);
} 
SolReturn sr = sol.send(command);
ObjectMapper om = new ObjectMapper();

%><%= om.writeValueAsString(sr) %>