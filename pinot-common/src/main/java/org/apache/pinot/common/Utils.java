/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common;

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.Attributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Utils {
  private Utils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  /**
   * Rethrows an exception, even if it is not in the method signature.
   *
   * @param t The exception to rethrow.
   */
  public static void rethrowException(Throwable t) {
    /* Error can be thrown anywhere and is type erased on rethrowExceptionInner, making the cast in
    rethrowExceptionInner a no-op, allowing us to rethrow the exception without declaring it. */
    Utils.<Error>rethrowExceptionInner(t);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void rethrowExceptionInner(Throwable exception)
      throws T {
    throw (T) exception;
  }

  /**
   * Obtains the name of the calling method and line number. This is slow, only use this for debugging!
   */
  public static String getCallingMethodDetails() {
    try {
      throw new RuntimeException();
    } catch (RuntimeException e) {
      return e.getStackTrace()[2].toString();
    }
  }

  private static final AtomicLong UNIQUE_ID_GEN = new AtomicLong(1);

  public static long getUniqueId() {
    return UNIQUE_ID_GEN.incrementAndGet();
  }

  /**
   * Takes a string, removes all characters that are not letters or digits and capitalizes the next letter following a
   * series of characters that are not letters or digits. For example, toCamelCase("Hello world!") returns "HelloWorld".
   *
   * @param text The text to camel case
   * @return The camel cased version of the string given
   */
  public static String toCamelCase(String text) {
    int length = text.length();
    StringBuilder builder = new StringBuilder(length);

    boolean capitalizeNextChar = false;

    for (int i = 0; i < length; i++) {
      char theChar = text.charAt(i);
      if (Character.isLetterOrDigit(theChar) || theChar == '.') {
        if (capitalizeNextChar) {
          builder.append(Character.toUpperCase(theChar));
          capitalizeNextChar = false;
        } else {
          builder.append(theChar);
        }
      } else {
        capitalizeNextChar = true;
      }
    }

    return builder.toString();
  }

  /**
   * Write the version of Pinot components to the log at info level.
   */
  public static void logVersions() {
    for (Map.Entry<String, String> titleVersionEntry : getComponentVersions().entrySet()) {
      LOGGER.info("Using {} {}", titleVersionEntry.getKey(), titleVersionEntry.getValue());
    }
  }

  /**
   * Obtains the version numbers of the Pinot components.
   *
   * @return A map of component name to component version.
   */
  public static Map<String, String> getComponentVersions() {
    Map<String, String> componentVersions = new HashMap<>();

    // Find the first URLClassLoader, walking up the chain of parent classloaders if necessary
    ClassLoader classLoader = Utils.class.getClassLoader();
    while (classLoader != null && !(classLoader instanceof URLClassLoader)) {
      classLoader = classLoader.getParent();
    }

    if (classLoader != null) {
      URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
      URL[] urls = urlClassLoader.getURLs();
      for (URL url : urls) {
        try {
          // Convert the URL to the JAR into a JAR URL: eg. jar:http://www.foo.com/bar/baz.jar!/ in order to load it
          URL jarUrl = new URL("jar", "", url + "!/");
          URLConnection connection = jarUrl.openConnection();
          if (connection instanceof JarURLConnection) {
            JarURLConnection jarURLConnection = (JarURLConnection) connection;

            // Read JAR attributes and log the Implementation-Title and Implementation-Version manifestvalues for pinot
            // components
            Attributes attributes = jarURLConnection.getMainAttributes();
            if (attributes != null) {
              String implementationTitle = attributes.getValue(Attributes.Name.IMPLEMENTATION_TITLE);
              String implementationVersion = attributes.getValue(Attributes.Name.IMPLEMENTATION_VERSION);
              if (implementationTitle != null && implementationTitle.contains("pinot")) {
                componentVersions.put(implementationTitle, implementationVersion);
              }
            }
          }
        } catch (IOException e) {
          // Ignored
        }
      }
    }

    return componentVersions;
  }
}
