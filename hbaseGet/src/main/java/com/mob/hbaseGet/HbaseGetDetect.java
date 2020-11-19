package com.mob.hbaseGet;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Hex;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import static java.util.stream.Collectors.toList;

public class HbaseGetDetect {
    public static String getTableName = null;

    public static String rowkeyFileName = null;

    public static int detectCnts = 0;

    public static List<String> lstRowkey = new ArrayList<>();

    public static Connection connection = null;

    public static boolean printLog;

    public static String zkQuorum;

    public static String keytab;

    public static String principal;

    public static int sleepSec;

/*
    private static final List<byte[]> columns = Lists.newArrayList(
            "c_45_1000",
            "c_338_1000",
            "c_505_1000",
            "c_509_1000",
            "c_710_1000",
            "c_735_1000",
            "c_785_1000",
            "c_1037_1000",
            "c_2980_1000",
            "c_3068_1000",
            "c_5350_1000",
            "c_3441_1000",
            "c_4440_1000",
            "c_5419_1000",
            "c_5429_1000",
            "c_1048_1000",
            "c_5710_1000",
            "c_45_1000",
            "c_338_1000",
            "c_505_1000",
            "c_509_1000",
            "c_710_1000",
            "c_735_1000",
            "c_785_1000",
            "c_1037_1000",
            "c_2980_1000",
            "c_3068_1000",
            "c_5350_1000",
            "c_3441_1000",
            "c_4440_1000",
            "c_5419_1000",
            "c_5429_1000",
            "c_1048_1000",
            "c_5710_1000").stream().map(n -> n.getBytes(StandardCharsets.UTF_8)).collect(toList());
*/

    private static final List<byte[]> columns = Lists.newArrayList(
            "c_7793_1000",
            "c_7794_1000",
            "c_7795_1000",
            "c_7796_1000",
            "c_7797_1000",
            "c_7798_1000",
            "c_7799_1000",
            "c_7800_1000",
            "c_7801_1000",
            "c_7802_1000",
            "c_7803_1000",
            "c_7804_1000",
            "c_7805_1000",
            "c_7806_1000",
            "c_7807_1000",
            "c_7808_1000",
            "c_7809_1000",
            "c_7810_1000",
            "c_7811_1000",
            "c_7812_1000",
            "c_7813_1000",
            "c_7814_1000",
            "c_7815_1000",
            "c_7816_1000",
            "c_7817_1000",
            "c_7818_1000",
            "c_7819_1000",
            "c_7820_1000",
            "c_7821_1000",
            "c_7822_1000",
            "c_7823_1000",
            "c_7824_1000",
            "c_7825_1000",
            "c_7826_1000",
            "c_7827_1000",
            "c_7828_1000",
            "c_7829_1000",
            "c_7830_1000",
            "c_7831_1000",
            "c_7832_1000",
            "c_7833_1000",
            "c_7834_1000",
            "c_7835_1000",
            "c_7836_1000",
            "c_7837_1000",
            "c_7838_1000",
            "c_7839_1000",
            "c_7840_1000",
            "c_7841_1000",
            "c_7842_1000",
            "c_7843_1000",
            "c_7844_1000",
            "c_7845_1000",
            "c_7846_1000",
            "c_7847_1000",
            "c_7848_1000",
            "c_7849_1000",
            "c_7850_1000",
            "c_7851_1000",
            "c_7852_1000",
            "c_7853_1000",
            "c_7854_1000",
            "c_7855_1000",
            "c_7856_1000",
            "c_7857_1000",
            "c_7858_1000",
            "c_7859_1000",
            "c_7860_1000",
            "c_7861_1000",
            "c_7862_1000",
            "c_7863_1000",
            "c_7864_1000",
            "c_7865_1000",
            "c_7866_1000",
            "c_7867_1000",
            "c_7868_1000",
            "c_7869_1000",
            "c_7870_1000",
            "c_7871_1000",
            "c_7872_1000",
            "c_7873_1000",
            "c_7874_1000",
            "c_7875_1000",
            "c_7876_1000",
            "c_7877_1000",
            "c_7878_1000",
            "c_7879_1000",
            "c_7880_1000",
            "c_7881_1000",
            "c_7882_1000",
            "c_7883_1000",
            "c_7884_1000",
            "c_7885_1000",
            "c_7886_1000",
            "c_7887_1000",
            "c_7888_1000",
            "c_7889_1000",
            "c_7890_1000",
            "c_7891_1000",
            "c_7892_1000").stream().map(n -> n.getBytes(StandardCharsets.UTF_8)).collect(toList());

    private static byte[] familyBytes = "c".getBytes(StandardCharsets.UTF_8);

    static {
        ResourceBundle resourceBundle = ResourceBundle.getBundle("hb");
        printLog = resourceBundle.getString("printLog").equals("1");
        zkQuorum = resourceBundle.getString("zkQuorum");
        resourceBundle.getString("sleepSec");
        sleepSec = Integer.parseInt(resourceBundle.getString("sleepSec"));
        System.out.println("zkQuorum:[" + zkQuorum + "],sleepSec=[" + sleepSec + "]");
    }

    public static void main(String[] args) throws IOException, Exception {
        if (args.length < 3) {
            System.out.println("usage HbaseGetDetect TableName rowkeyFileName detectCnts");
            System.exit(2);
        }
        getTableName = args[0];
        rowkeyFileName = args[1];
        detectCnts = Integer.parseInt(args[2]);
        System.out.println("getTableName:[" + getTableName + "], rowkeyFileName=[" + rowkeyFileName + "]");
        if (init() < 0)
            System.exit(2);
        doDetect();
        destroy();
    }

    public static void destroy() {
        if (connection != null)
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    public static int init() {
        System.out.println("-----enter  init ----- ");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.client.retries.number", "1");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            FileInputStream in = new FileInputStream(rowkeyFileName);
            InputStreamReader inReader = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader bufReader = new BufferedReader(inReader);
            String line = null;
            int i = 1;
            while ((line = bufReader.readLine()) != null) {
                lstRowkey.add(line);
                i++;
            }
            bufReader.close();
            inReader.close();
            in.close();
            System.out.println("rowkey sample counts:" + lstRowkey.size());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("read " + rowkeyFileName + " error!");
        }
        return 0;
    }

    public static void doDetect() throws Exception, IOException {
        int rowkeyCnts = lstRowkey.size();
        int MAX = rowkeyCnts - 1;
        int MIN = 0;
        Random rand = new Random();
        int randomIdx = 0;
        Table table = connection.getTable(TableName.valueOf(getTableName));
        if (printLog)
            System.out.println("**get table ok !");
        double time = 0.0D;
        String columnFamily = null;
        String columnName = null;
        String columnValue = null;
        int columnCnt = 0;
        for (int i = 0; i < detectCnts; i++) {
            long start = System.currentTimeMillis();
            randomIdx = rand.nextInt(MAX - MIN + 1) + MIN;
            String key = UUID.randomUUID().toString().replace("-", "");
//            String key = lstRowkey.get(randomIdx);
//            Get get = new Get(Bytes.toBytes(key));
            Get get = new Get(Hex.decodeHex(key.toCharArray()));
            for (byte[] column : columns) {
                get.addColumn(familyBytes, column);
            }

            Result result = table.get(get);
            if (!result.isEmpty()) {
                List<Cell> listCells = result.listCells();
                columnCnt = 0;
                for (Cell cell : listCells) {
                    columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                    columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    columnCnt++;
                }
            }
            table.close();
            long end = System.currentTimeMillis();
            time = (end - start) / 1000.0D;
            System.out.println(new Date() + "###" + i + "###  getTableName:[" + getTableName + "],key:[" + key + "], time:[" + time + "] second,columnCnt:[" + columnCnt + "]");
            Thread.sleep((sleepSec * 1000));
        }
        System.out.println("**hbase query finished !**");
    }
}

