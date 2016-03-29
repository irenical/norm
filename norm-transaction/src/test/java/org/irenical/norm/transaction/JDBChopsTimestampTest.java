package org.irenical.norm.transaction;

import org.junit.*;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Function;

public class JDBChopsTimestampTest {

  private static final String SQL_INSERT_ARTICLE = "INSERT INTO article (title, published_stamp) VALUES(?,?)";
  private static final String SQL_SELECT_ARTICLE_BY_ID = "SELECT published_stamp FROM article WHERE article_id = ?";
  private static final long TEST_DATE = 1459189283000l;
  private static final ZoneId TEST_ZONE_ID = ZoneId.of("America/Los_Angeles");

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


  private int insertArticle(Connection connection, String title, Object publishedDate) throws SQLException {
    int articleId;
    try (PreparedStatement ins = connection.prepareStatement(SQL_INSERT_ARTICLE, Statement.RETURN_GENERATED_KEYS)) {
      JDBChops.prepareInput(ins, Arrays.asList(title, publishedDate));
      ins.executeUpdate();
      try (ResultSet generatedKeys = ins.getGeneratedKeys()) {
        if (generatedKeys.next()) {
          articleId = generatedKeys.getInt(1);
        } else {
          throw new SQLException("Failed to insert article");
        }
      }
    }
    return articleId;
  }

  private void selectArticle(Connection connection, int articleId, Function<ResultSet,Boolean> callback) throws SQLException {
    try (PreparedStatement sel = connection.prepareStatement(SQL_SELECT_ARTICLE_BY_ID)) {
      JDBChops.prepareInput(sel, Collections.singletonList(articleId));
      sel.execute();
      try (ResultSet resultSet = sel.getResultSet()) {
        boolean result = callback.apply(resultSet);
        if (!result) {
          throw new SQLException("Failed to get values from resultSet");
        }
      }
    }
  }


  @Test
  public void testSetInputTimestamp() throws Exception {
    try (Connection conn = getConnection()) {
      Timestamp testTimestamp = new Timestamp(TEST_DATE);
      int articleId = insertArticle(conn, "Foo", testTimestamp);

      selectArticle(conn, articleId, (resultSet) -> {
        boolean result = false;
        try {
          if (resultSet.next()) {
            //Timestamp was stored and read using the jvm default timezone
            Timestamp storedTimestamp = resultSet.getTimestamp(1);
            Assert.assertEquals(testTimestamp, storedTimestamp);
            result = true;
          }
        } catch (SQLException e) {
          // unable to validate timestamp
        }
        return result;
      });
    }
  }

  @Test
  public void testSetInputZonedDateTime() throws Exception {
    try (Connection conn = getConnection()) {
      ZonedDateTime testZdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(TEST_DATE), TEST_ZONE_ID);
      int articleId = insertArticle(conn, "Foo", testZdt);

      selectArticle(conn, articleId, (resultSet) -> {
        boolean result = false;
        try {
          if (resultSet.next()) {
            //Timestamp was stored and read using the timezone provided by to the ZonedDateTime
            Timestamp storedTimestamp = resultSet.getTimestamp(1, GregorianCalendar.from(testZdt));

            Timestamp originalTimestamp = Timestamp.from(testZdt.toInstant());
            Assert.assertEquals(originalTimestamp, storedTimestamp);

            ZonedDateTime storedZdt = ZonedDateTime.ofInstant(storedTimestamp.toInstant(), testZdt.getZone());
            Assert.assertEquals(testZdt, storedZdt);
            result = true;
          }
        } catch (SQLException e) {
          //unable to validate timestamp
        }
        return result;
      });
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
