package com.ocean.utils;

import java.io.File;

/**
 * @author 徐正洲
 * @create 2023-01-05 10:21
 */
public class CkFileUtil {

    public static String getMaxTimeFileName(File file) {
        Long fileTime = 0L;
        File fileSubName = null;
        String  fileChildName = null;
        File[] files = file.listFiles();

        for (int i = 0; i < files.length; i++) {
            if (fileTime < files[i].lastModified()) {
                fileTime = files[i].lastModified();
                fileSubName = files[i];
            }
        }
        File[] fileChildNames = fileSubName.listFiles();
        for (int i = 0; i < fileChildNames.length; i++) {
            if (fileChildNames[i].getName().startsWith("chk-")){
                fileChildName =  fileChildNames[i].toString();
            }
        }
        return fileChildName;
    }
}
