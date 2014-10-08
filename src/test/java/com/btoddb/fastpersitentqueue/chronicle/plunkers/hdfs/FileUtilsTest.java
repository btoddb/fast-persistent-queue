package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;


import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


public class FileUtilsTest {

    @Test
    public void testInsertTimestampWithExt() {
        FileUtils fileUtils = new FileUtils();
        assertThat(fileUtils.insertTimestamp("filename.ext"), is("filename.${timestamp}.ext"));
    }

    @Test
    public void testInsertTimestampWithoutExt() {
        FileUtils fileUtils = new FileUtils();
        assertThat(fileUtils.insertTimestamp("filename"), is("filename.${timestamp}"));
    }

}