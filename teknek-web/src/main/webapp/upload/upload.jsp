<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
    <%@page import="org.apache.commons.fileupload.*,org.apache.commons.fileupload.servlet.*,org.apache.commons.fileupload.disk.*,java.util.*,java.io.*" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Insert title here</title>
</head>
<body>

<% 
boolean isMultipart = ServletFileUpload.isMultipartContent(request);
File uploadedFile = null;
if (isMultipart){
	DiskFileItemFactory factory = new DiskFileItemFactory();
	File repository = new File("/tmp/teknek-web-upload-tmp");
	if (!repository.exists()){
	  repository.mkdir();
	}
	File finalDir = new File("/tmp/teknek-web-upload-final");
	if (!finalDir.exists()){
	  finalDir.mkdir();
	}
	factory.setRepository(repository);
	ServletFileUpload upload = new ServletFileUpload(factory);
	List<FileItem> items = upload.parseRequest(request);
	Iterator<FileItem> iter = items.iterator();
	while (iter.hasNext()) {
	    FileItem item = iter.next();
	    if (item.isFormField()) {
	      String name = item.getFieldName();
	      String value = item.getString();
	    } else {
	      String fieldName = item.getFieldName();
	      String fileName = item.getName();
	      String contentType = item.getContentType();
	      boolean isInMemory = item.isInMemory();
	      long sizeInBytes = item.getSize();
	      uploadedFile = new File(finalDir,fileName);
	      item.write(uploadedFile);
	    }
	}
}
%>

<% if (uploadedFile != null) { %>
	<font color=RED">File <%=uploadedFile.getName()%> uploaded</font><br>
	<%= request.getSession().getServletContext().getContextPath()+"/Serve/"+uploadedFile.getName() %><br>
<% } %>

<form method="POST" enctype="multipart/form-data">
  File to upload: <input type="file" name="upfile"><br/>
  Notes about the file: <input type="text" name="note"><br/>
  <br/>
  <input type="submit" value="Press"> to upload the file!
</form>
</body>
</html>