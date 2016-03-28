package org.irenical.norm.transaction;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;

import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class JDBChopsTest {

  private PreparedStatement mockPS;

  private static final int TEST_IDX = 0;

  enum TestEnum {
    TEST_VALUE_1
  }


  @Before
  public void setUp() {
    mockPS = mock(PreparedStatement.class);
  }


  @Test
  public void testSetInputTimestamp() throws Exception {
    Timestamp t = new Timestamp(System.currentTimeMillis());
    JDBChops.setInput(mockPS, TEST_IDX, t);

    verify(mockPS).setTimestamp(
            eq(TEST_IDX),
            eq(t)
    );
  }


  @Test
  public void testSetInputZonedDateTime() throws Exception {
    ZonedDateTime zdt = ZonedDateTime.now();
    JDBChops.setInput(mockPS, TEST_IDX, zdt);

    Timestamp expectedTimestamp = Timestamp.from(zdt.toInstant());
    ArgumentMatcher<Calendar> calendarArgumentMatcher = new TimeZoneMatcher(zdt.getZone());
    verify(mockPS).setTimestamp(
            eq(TEST_IDX),
            eq(expectedTimestamp),
            argThat(calendarArgumentMatcher)
    );
  }


  @Test
  public void testSetInputTime() throws Exception {
    Time t = new Time(System.currentTimeMillis());
    JDBChops.setInput(mockPS, TEST_IDX, t);

    verify(mockPS).setTime(
            eq(TEST_IDX),
            eq(t)
    );
  }


  @Test
  public void testSetInputDate() throws Exception {
    Date d = new Date(System.currentTimeMillis());
    JDBChops.setInput(mockPS, TEST_IDX, d);

    verify(mockPS).setDate(
            eq(TEST_IDX),
            eq(d)
    );
  }


  @Test
  public void testSetInputEnum() throws Exception {
    TestEnum testEnum = TestEnum.TEST_VALUE_1;
    JDBChops.setInput(mockPS, TEST_IDX, testEnum);

    verify(mockPS).setString(
            eq(TEST_IDX),
            eq(String.valueOf(testEnum))
    );
  }


  @Test
  public void testSetInputObject() throws Exception {
    Object o = new Object();
    JDBChops.setInput(mockPS, TEST_IDX, o);

    verify(mockPS).setObject(
            eq(TEST_IDX),
            eq(o)
    );
  }


  /**
   * Compare the timezone of an argument of type Calendar with a provided ZoneId
   */
  class TimeZoneMatcher extends ArgumentMatcher<Calendar> {

    private final ZoneId zoneId;

    TimeZoneMatcher(ZoneId zoneId) {
      this.zoneId = zoneId;
    }

    @Override
    public boolean matches(Object o) {
      return zoneId.equals(((Calendar) o).getTimeZone().toZoneId());
    }

  }

}
