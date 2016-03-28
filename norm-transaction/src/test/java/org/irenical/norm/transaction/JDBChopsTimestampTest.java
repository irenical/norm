package org.irenical.norm.transaction;

import org.junit.*;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class JDBChopsTimestampTest {

  private static final String SQL_INSERT_ARTICLE = "INSERT INTO article (title, published_stamp) VALUES(?,?)";
  private static final String SQL_SELECT_ARTICLE_BY_ID = "SELECT published_stamp FROM article WHERE article_id = ?";
  private static final long SUMMER_DATE = 1459189283000l;

  private static Connection getConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:derby:memory:norm_testing_timestamp;create=true");
  }

  @BeforeClass
  public static void init() throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
    Connection connection = getConnection();

    // create table
    PreparedStatement createPeopleTableStatement = connection.prepareStatement(
            "CREATE TABLE ARTICLE (ARTICLE_ID INT NOT NULL GENERATED ALWAYS AS IDENTITY CONSTRAINT ARTICLE_PK PRIMARY KEY," +
                    " TITLE VARCHAR(26)," +
                    " PUBLISHED_STAMP TIMESTAMP)");
    createPeopleTableStatement.executeUpdate();
    createPeopleTableStatement.close();

    connection.close();
  }


  @Test
  public void testSetInputTimestamp() throws Exception {
    try (Connection conn = getConnection()) {
      int articleId;
      Timestamp testTimestamp = new Timestamp(SUMMER_DATE);

      try (PreparedStatement ins = conn.prepareStatement(SQL_INSERT_ARTICLE, Statement.RETURN_GENERATED_KEYS)) {
        JDBChops.prepareInput(ins, Arrays.asList("Foo", testTimestamp));
        ins.executeUpdate();
        try (ResultSet generatedKeys = ins.getGeneratedKeys()) {
          if (generatedKeys.next()) {
            articleId = generatedKeys.getInt(1);
          } else {
            Assert.fail();
            return;
          }
        }
      }

      try (PreparedStatement sel = conn.prepareStatement(SQL_SELECT_ARTICLE_BY_ID)) {
        JDBChops.prepareInput(sel, Collections.singletonList(articleId));
        sel.execute();
        try (ResultSet resultSet = sel.getResultSet()) {
          if (resultSet.next()) {
            Timestamp result = resultSet.getTimestamp(1);
            Assert.assertEquals(testTimestamp, result);
          } else {
            Assert.fail();
          }
        }
      }
    }
  }

  @Test
  public void testSetInputZonedDateTime() throws Exception {
    try (Connection conn = getConnection()) {
      int articleId;

      ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(SUMMER_DATE), ZoneId.of("Europe/Lisbon"));

      try (PreparedStatement ins = conn.prepareStatement(SQL_INSERT_ARTICLE, Statement.RETURN_GENERATED_KEYS)) {
        JDBChops.prepareInput(ins, Arrays.asList("Foo", zdt));
        ins.executeUpdate();
        try (ResultSet generatedKeys = ins.getGeneratedKeys()) {
          if (generatedKeys.next()) {
            articleId = generatedKeys.getInt(1);
          } else {
            Assert.fail();
            return;
          }
        }
      }

      try (PreparedStatement sel = conn.prepareStatement(SQL_SELECT_ARTICLE_BY_ID)) {
        JDBChops.prepareInput(sel, Collections.singletonList(articleId));
        sel.execute();
        try (ResultSet resultSet = sel.getResultSet()) {
          if (resultSet.next()) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));

            Timestamp result = resultSet.getTimestamp(1, GregorianCalendar.from(zdt));

            ZonedDateTime resultZdt = ZonedDateTime.ofInstant(
                    Instant.ofEpochMilli(result.getTime()),
                    ZoneId.of("Europe/Lisbon"));


            Assert.assertTrue(zdt.equals(resultZdt));

          } else {
            Assert.fail();
          }
        }
      }
    }

  }

  @AfterClass
  public static void tearDown() throws Exception {

    try {
      DriverManager.getConnection("jdbc:derby:memory:norm_testing_timestamp;drop=true");
    } catch (SQLException e) {
      //When you drop the database, Derby issues what appears to be an error but is actually an indication of success. You need to catch error 08006, as described in "The WwdEmbedded program" in Getting Started with Derby. https://db.apache.org/derby/docs/10.8/devguide/cdevdvlpinmemdb.html
    }

  }
}
