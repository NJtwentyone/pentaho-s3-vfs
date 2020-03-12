package org.pentaho.s3common;

import org.junit.*;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.util.StorageUnitConverter;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3CommonPipedOutputStreamTest {

  @BeforeClass
  public static void initKettle() throws Exception {
    KettleEnvironment.init( false );
  }

  @Test
  public void testReadPartSize() throws Exception {
    S3CommonPipedOutputStream s3CommonPipedOutputStream = getTestInstance();
    S3KettleProperty s3KettleProperty = mock( S3KettleProperty.class );
    when( s3KettleProperty.getPartSize() ).thenReturn( "10MB" );
    s3CommonPipedOutputStream.s3KettleProperty = s3KettleProperty;

    assertEquals( 10 * 1024 * 1024, s3CommonPipedOutputStream.readPartSize() );
  }

  @Test
  public void testConvertToInt() throws Exception {
    S3CommonPipedOutputStream s3CommonPipedOutputStream = getTestInstance();

    // TEST 1: below int max
    assertEquals( 10, s3CommonPipedOutputStream.convertToInt( 10L ) );

    // TEST 2: at int max
    assertEquals( Integer.MAX_VALUE, s3CommonPipedOutputStream.convertToInt( (long) Integer.MAX_VALUE ) );

    // TEST 3: above int max
    assertEquals( Integer.MAX_VALUE, s3CommonPipedOutputStream.convertToInt( 5L* 1024L * 1024L * 1024L ) );
  }

  @Test
  public void testParsePartSize() throws Exception {
    S3CommonPipedOutputStream s3CommonPipedOutputStream = getTestInstance();
    s3CommonPipedOutputStream.storageUnitConverter = new StorageUnitConverter();
    long _5MBLong = 5L * 1024L * 1024L;
    long _124MBLong = 124L * 1024L * 1024L;
    long _5GBLong = 5L * 1024L * 1024L * 1024L;
    long _12GBLong = 12L * 1024L * 1024L * 1024L;
    long minimumPartSize = _5MBLong;
    long maximumPartSize = _5GBLong;


    // TEST 1: below minimum
    assertEquals( minimumPartSize, s3CommonPipedOutputStream.parsePartSize( "1MB" ) );

    // TEST 2: at minimum
    assertEquals( minimumPartSize, s3CommonPipedOutputStream.parsePartSize( "5MB" ) );

    // TEST 3: between minimum and maximum
    assertEquals( _124MBLong, s3CommonPipedOutputStream.parsePartSize( "124MB" ) );

    // TEST 4: at maximum
    assertEquals( maximumPartSize, s3CommonPipedOutputStream.parsePartSize( "5GB" ) );

    // TEST 5: above maximum
    assertEquals( _12GBLong, s3CommonPipedOutputStream.parsePartSize( "12GB" ) );
  }

  @Test
  public void testConvertToLong() throws Exception  {
    S3CommonPipedOutputStream s3CommonPipedOutputStream = getTestInstance();
    long _10MBLong = 10L * 1024L * 1024L;
    s3CommonPipedOutputStream.storageUnitConverter = new StorageUnitConverter();
    assertEquals( _10MBLong, s3CommonPipedOutputStream.convertToLong( "10MB" ) );
  }

  public  S3CommonPipedOutputStream getTestInstance() throws Exception {
    return new S3CommonPipedOutputStream( null, null, null );
  }
}