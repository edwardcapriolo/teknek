package io.teknek.sol;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class Serve
 */
public class Serve extends HttpServlet {
	private static final long serialVersionUID = 1L;

    /**
     * Default constructor. 
     */
    public Serve() {
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	  doService(request,response);
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doService(request,response);
	}

  protected void doService(HttpServletRequest request, HttpServletResponse response)
          throws ServletException, IOException {
    String path = request.getPathInfo();

    File f = new File("/tmp/teknek-web-upload-final" + path);
    //response.setContentType("application/java-archive");
    response.setContentType(getServletContext().getMimeType(f.getName()));
    response.setContentLength((int)f.length());
    response.setHeader("Content-Disposition", "inline; filename=\"" + f.getName() + "\"");
    try (ServletOutputStream output = response.getOutputStream();
            InputStream input = new FileInputStream(new File("/tmp/teknek-web-upload-final" + path));) {
      // transfer input stream to output stream, via a buffer
      byte[] buffer = new byte[2048];
      int bytesRead;
      while ((bytesRead = input.read(buffer)) != -1) {
        output.write(buffer, 0, bytesRead);
      }
    }
  }
}


/**
 * protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
{
    String filename = URLDecoder.decode(request.getPathInfo(), "UTF-8");
    File file = new File("/path/to/images", filename);

    response.setContentType(getServletContext().getMimeType(file.getName()));
    response.setContentLength(file.length());
    response.setHeader("Content-Disposition", "inline; filename=\"" + file.getName() + "\"");

    BufferedInputStream input = null;
    BufferedOutputStream output = null;

    try {
        input = new BufferedInputStream(new FileInputStream(file));
        output = new BufferedOutputStream(response.getOutputStream());

        byte[] buffer = new byte[8192];
        int length;
        while ((length = input.read(buffer)) > 0) {
            output.write(buffer, 0, length);
        }
    } finally {
        if (output != null) try { output.close(); } catch (IOException ignore) {}
        if (input != null) try { input.close(); } catch (IOException ignore) {}
    }
}
*/
