package com.geek.hm.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HBaseHM {
    static String colFamily = "info";
    static String colFamily2 = "score";

    static String myNs = "icon";
    static Boolean myNsIsExisted = false;

    static TableName tableName = TableName.valueOf("icon:student");
    static List<String> student_ids = new ArrayList<>();
    static List<String> classes = new ArrayList<>();
    static List<String> understanding = new ArrayList<>();
    static List<String> programming = new ArrayList<>();
    static List<String> rowKeys = new ArrayList<>();

    static {
        Collections.addAll(student_ids, "20210000000001", "20210000000002", "20210000000003", "20210000000004", "G20220735020136");
        Collections.addAll(classes, "1", "1", "2", "2", "geek");
        Collections.addAll(understanding, "75", "85", "80", "60", "98");
        Collections.addAll(programming, "82", "67", "80", "61", "98");
        Collections.addAll(rowKeys, "Tom", "Jerry", "Jack", "Rose", "icon");
    }

    public static void main(String[] args) throws IOException {
        String arg = args[0];
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", arg);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", arg + ":60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();


        //list_namespace
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();

        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            String name = namespaceDescriptor.getName();
            System.out.println(name);
        }
        Arrays.stream(namespaceDescriptors).forEach((nsd)->{
            if (nsd.getName().equals(myNs)){
                myNsIsExisted = true;
            }
        });
        if (!myNsIsExisted){
            // 创建命名空间
            admin.createNamespace(NamespaceDescriptor.create("icon").build());
//        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create("icon");
//        NamespaceDescriptor build = builder.build();
//        admin.createNamespace(build);
        }


        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
            HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(colFamily2);
            hTableDescriptor.addFamily(hColumnDescriptor);
            hTableDescriptor.addFamily(hColumnDescriptor2);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        // 插入数据
        for (int i = 0; i < rowKeys.size(); i++) {
            List<String> vl = new ArrayList<>();
            Collections.addAll(vl, student_ids.get(i), classes.get(i), understanding.get(i), programming.get(i));
            Put put = addRow(rowKeys.get(i), vl);
            conn.getTable(tableName).put(put);
            System.out.println("Data insert success");
        }

        // 查看数据
        for (String rowKey : rowKeys) {
            readRows(rowKey, conn);
        }

        // 删除数据
        for (String rowKey : rowKeys) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
            conn.getTable(tableName).delete(delete);
            System.out.println("Delete Success");
        }

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }

    public static Put addRow(String rowKey, List<String> valueList) {
        // 插入数据
        Put put = new Put(Bytes.toBytes(rowKey)); // row key
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("stundent_id"), Bytes.toBytes(valueList.get(0))); // col1
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("class"), Bytes.toBytes(valueList.get(1))); // col1

        put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("understanding"), Bytes.toBytes(valueList.get(2))); // col2
        put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("programming"), Bytes.toBytes(valueList.get(3))); // col2
        return put;
    }

    public static void readRows(String rowKey, Connection conn) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
//                System.out.println(cell);
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }
    }

}
