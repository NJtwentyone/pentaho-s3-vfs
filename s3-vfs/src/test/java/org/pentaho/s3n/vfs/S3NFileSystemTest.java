/*!
* Copyright 2010 - 2019 Hitachi Vantara.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/
package org.pentaho.s3n.vfs;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.UserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.s3.vfs.S3FileName;
import org.pentaho.s3.vfs.S3FileNameTest;
import org.pentaho.s3.vfs.S3FileProvider;
import org.pentaho.s3.vfs.S3FileSystem;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for S3FileSystem
 */
public class S3NFileSystemTest {

  S3NFileSystem fileSystem;
  S3NFileName fileName;

  @Before
  public void setUp() throws Exception {
    fileName = new S3NFileName(
      S3FileNameTest.SCHEME,
      "/",
      "",
      FileType.FOLDER );
    fileSystem = new S3NFileSystem( fileName, new FileSystemOptions() );
  }


  @Test
  public void testCreateFile() throws Exception {
    assertNotNull( fileSystem.createFile( new S3FileName( "s3n", "bucketName", "/bucketName/key", FileType.FILE ) ) );
  }

  @Test
  public void testGetS3Service() throws Exception {
    assertNotNull( fileSystem.getS3Client() );

    FileSystemOptions options = new FileSystemOptions();
    UserAuthenticator authenticator = mock( UserAuthenticator.class );
    UserAuthenticationData authData = mock( UserAuthenticationData.class );
    when( authenticator.requestAuthentication( S3FileProvider.AUTHENTICATOR_TYPES ) ).thenReturn( authData );
    when( authData.getData( UserAuthenticationData.USERNAME ) ).thenReturn( "username".toCharArray() );
    when( authData.getData( UserAuthenticationData.PASSWORD ) ).thenReturn( "password".toCharArray() );
    DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator( options, authenticator );

    fileSystem = new S3NFileSystem( fileName, options );
    assertNotNull( fileSystem.getS3Client() );
  }
}
