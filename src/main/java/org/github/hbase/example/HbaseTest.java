package org.github.hbase.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class HbaseTest {

    String name = "phone";
    TableName tbl = TableName.valueOf(name);


    Configuration conf = null;
    Connection conn = null;

    Admin admin;

    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();



    }


    @Test
    public void createTable() throws IOException {
        if (admin.tableExists(tbl)) {
            admin.disableTable(tbl);
            admin.deleteTable(tbl);
        }

        TableDescriptor desc = TableDescriptorBuilder
                .newBuilder(tbl)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("cf".getBytes()).build())
                .build();
        admin.createTable(desc);

    }

    @Test
    public void insertTbl() throws IOException {
        Table table = conn.getTable(tbl);
        String rowKey = "20011234";
        Put put = new Put(rowKey.getBytes());
        put.addColumn("cf".getBytes(),"name".getBytes(), "abehsu".getBytes());
        put.addColumn("cf".getBytes(),"age".getBytes(), "12".getBytes());
        put.addColumn("cf".getBytes(),"sex".getBytes(), "male".getBytes());
        table.put(put);
    }

    @After
    public void destory() throws IOException {
        if (admin != null) {
            admin.close();
        }
    }
}

