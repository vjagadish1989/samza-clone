package org.apache.samza.clustermanager;

import org.apache.samza.coordinator.server.HttpServer;
import org.eclipse.jetty.servlet.ServletHolder;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.samza.coordinator.server.HttpServer;
import org.eclipse.jetty.servlet.ServletHolder;

import java.net.MalformedURLException;
import java.net.URL;

public class MockHttpServer extends HttpServer {

  public MockHttpServer(String rootPath, int port, String resourceBasePath, ServletHolder defaultHolder)
  {
    super(rootPath, port, resourceBasePath, defaultHolder);
    start();
  }

  @Override
  public void start() {
    super.running_$eq(true);
  }

  @Override
  public void stop() {
    super.running_$eq(false);
  }

  @Override
  public URL getUrl() {
    if(running()) {
      try {
        System.out.println("returning val");

        return new URL("http://localhost:12345/");
      } catch (MalformedURLException mue) {
        mue.printStackTrace();
      }
    }
    System.out.println("returning null");
    return null;
  }
}
