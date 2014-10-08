package com.btoddb.fastpersitentqueue.chronicle.plunkers.hdfs;

import com.btoddb.fastpersitentqueue.chronicle.TokenizedFilePath;
import org.apache.commons.io.FilenameUtils;


/**
 * Created by burrb009 on 10/6/14.
 */
public class FileUtils {

    public String concatPath(String pathPattern, String segment) {
        if (pathPattern.endsWith("/")) {
            return pathPattern + segment;
        }
        else {
            return pathPattern + "/" + segment;
        }
    }

    public String insertTimestamp(String fn) {
        int index;
        if (-1 != (index= FilenameUtils.indexOfExtension(fn))) {
            return fn.substring(0, index) + ".${timestamp}" + fn.substring(index);
        }
        else {
            return fn + ".${timestamp}";
        }
    }

}
