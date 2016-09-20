package info.puton.practice.hbase;

import info.puton.practice.hbase.util.FileUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static info.puton.practice.hbase.util.FileUtil.getFileBytes;

/**
 * Created by taoyang on 2016/9/20.
 */
public class HBaseFileStore {

    static class TABLE {
        static String FILE = "file_tb";
    }

    static class COLUMN_FAMILY {
        static String FILE = "file_cf";
    }

    static class COLUMN_QUALIFIER {
        static String NAME = "name";
        static String TYPE = "type";
        static String DATA = "data";
    }

    public static void deleteTable() throws Exception {
        HBaseDao.deleteTable(TABLE.FILE);
    }

    public static void createTable() throws Exception {
        HBaseDao.createTable(TABLE.FILE, COLUMN_FAMILY.FILE);
    }

    public static void putFile(String filePath, String key) throws Exception {
//        HBaseDao.createTable(TABLE.FILE, COLUMN_FAMILY.FILE);
        String name = FileUtil.getFileName(filePath);
        String type = FileUtil.getFileSuffix(filePath);
        byte[] data = getFileBytes(filePath);
        HBaseDao.putCell(TABLE.FILE, key, COLUMN_FAMILY.FILE, COLUMN_QUALIFIER.NAME, name);
        HBaseDao.putCell(TABLE.FILE, key, COLUMN_FAMILY.FILE, COLUMN_QUALIFIER.TYPE, type);
        HBaseDao.putCell(TABLE.FILE, key, COLUMN_FAMILY.FILE, COLUMN_QUALIFIER.DATA, data);
    }

    public static void getFile(String fileLocation, String key) throws Exception {
        String name = HBaseDao.getStringCell(TABLE.FILE, key, COLUMN_FAMILY.FILE, COLUMN_QUALIFIER.NAME);
        String type = HBaseDao.getStringCell(TABLE.FILE, key, COLUMN_FAMILY.FILE, COLUMN_QUALIFIER.TYPE);
        byte[] data = HBaseDao.getBytesCell(TABLE.FILE, key, COLUMN_FAMILY.FILE, COLUMN_QUALIFIER.DATA);
        String filePath = fileLocation + "/" + name + "." + type;
        File file = new File(filePath);
        FileUtils.writeByteArrayToFile(file, data);
    }

    public static void main(String[] args) {
        try {

//            HBaseFileStore.deleteTable();

            String filePath = "D:/programming/java/practice/hbase-practice/src/main/resources/一碗阳春面.docx";
//            putFile(filePath, "a00002");
            getFile("E:/tmp/yang","a00002");
//            System.out.println(HBaseDao.getCell("ss_file", "000001", "ct", "data"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
