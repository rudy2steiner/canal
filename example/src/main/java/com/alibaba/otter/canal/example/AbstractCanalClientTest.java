package com.alibaba.otter.canal.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.model.ChangeRecord;
import com.alibaba.otter.canal.model.ColumnChangeData;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionBegin;
import com.alibaba.otter.canal.protocol.CanalEntry.TransactionEnd;
import com.alibaba.otter.canal.protocol.Message;

import com.google.common.primitives.Bytes;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 测试基类
 * 
 * @author jianghang 2013-4-15 下午04:17:12
 * @version 1.0.4
 */
public class AbstractCanalClientTest {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractCanalClientTest.class);
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    protected static String context_format = null;
    protected static String row_format = null;
    protected static String transaction_format = null;

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                     + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms"
                     + SEP;

        transaction_format = SEP + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms" + SEP;

    }

    protected volatile boolean running = false;
    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };
    protected Thread thread = null;
    protected CanalConnector connector;
    protected String destination;

    public AbstractCanalClientTest(String destination){
        this(destination, null);
    }

    public AbstractCanalClientTest(String destination, CanalConnector connector){
        this.destination = destination;
        this.connector = connector;
    }

    protected void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {

            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        MDC.remove("destination");
    }

    protected void process() {
        int batchSize = 5 * 1024;
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        // try {
                        // Thread.sleep(1000);
                        // } catch (InterruptedException e) {
                        // }
                    } else {
                        printSummary(message, batchId, size);
                        printEntry(message.getEntries());
                    }

                    connector.ack(batchId); // 提交确认
                    // connector.rollback(batchId); // 处理失败, 回滚数据
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    private void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format,
            new Object[] { batchId, size, memsize, format.format(new Date()), startPosition, endPosition });
    }

    protected String buildPositionForDump(Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
               + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
    }

    /**
     * 用于2017年阿里外部赛复赛使用的方法，将增量变更信息转化成比赛数据,并且写到外部文件
     * 
     * @param entry
     */
    protected void analyseEntryAndFlushToFile(Entry entry) {

        RowChange rowChage = null;
        try {
            rowChage = RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
        }

        // ----------1. 初始化ChangeRecord所需的成员变量---------------

        // 时间戳，单位毫秒
        String timestamp = String.valueOf(entry.getHeader().getExecuteTime());
        // schema
        String schema = entry.getHeader().getSchemaName();
        // table
        String table = entry.getHeader().getTableName();
        // 变更类型
        String type = null;
        switch (rowChage.getEventType()) {
            case INSERT:
                type = "I";
                break;
            case UPDATE:
                type = "U";
                break;
            case DELETE:
                type = "D";
                break;
            default:
                type = "Undefined";

        }
        if ("Undefined".equals(type)) {
            throw new RuntimeException("event type is not defined , data:" + entry.toString());
        }

        // ----------2. 初始化ColumnChangeData List ---------------
        //创建个byte数组
        byte[] resultBytes=new byte[0];
        logger.info("#################### List Size is:"+rowChage.getRowDatasList().size()+"  type:"+type+"##########\n");
        RowData rowData;
        for (int i = 0; i < rowChage.getRowDatasList().size(); i++) {
            // for (RowData rowData : rowChage.getRowDatasList()) {
            // id由binlog文件名和offset组成
            rowData = rowChage.getRowDatasList().get(i);
            long basicOffset = Long.valueOf(entry.getHeader().getLogfileOffset());
            long realOffset = basicOffset + i;
            String id = entry.getHeader().getLogfileName() + String.valueOf(realOffset);
            List<ColumnChangeData> changeDataList = new ArrayList<ColumnChangeData>();
            if ("I".equals(type)) {
                // 只有更新后的值
                for (Column column : rowData.getAfterColumnsList()) {
                    changeDataList.add(transferColumn(column, true));
                }

            } else if ("U".equals(type)) {
                // 有更新前后的值
                List columnNewList = rowData.getAfterColumnsList();
                List columnOldList = rowData.getBeforeColumnsList();
                changeDataList = transferColumnSetBoth(columnNewList, columnOldList);
            } else if ("D".equals(type)) {
                // 只有变更后的值
                for (Column column : rowData.getBeforeColumnsList()) {
                    changeDataList.add(transferColumn(column, false));
                }
            }

            // ----------3. 构造ChangeRecord ---------------
            ChangeRecord changeRecord = new ChangeRecord(id, timestamp, schema, table, type, changeDataList);
            String resultStr = changeRecord.toString() + "\n";
            //合并两个byte数组
            resultBytes= Bytes.concat(resultBytes,resultStr.getBytes());

        }
        storeChangeToDisk(resultBytes);
    }

    /**
     * 将结果写入外部文件，采用mapped memoery
     * 
     * @param resultBytes
     */
    protected void storeChangeToDisk(byte[] resultBytes) {
        File destFile = new File("/Users/wanshao/work/canal_data/canal.txt");

        FileChannel fileInputChannel = null;
        try {
            if(!destFile.exists()){
                destFile.createNewFile();
            }

            //获取可写的file channel；使用FileInputStream是只读
            RandomAccessFile raf   = new RandomAccessFile(destFile,"rw");
            //设置指针位置为文件末尾
            long fileLength = raf.length();
            raf.seek(fileLength);
            fileInputChannel = raf.getChannel();

            //映射的字节数
            long size = 1024*1024;
            MappedByteBuffer buf = fileInputChannel.map(MapMode.READ_WRITE, fileInputChannel.position(), size);
            buf.put(resultBytes);
            buf.force();
            fileInputChannel.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 得到ColumnChangeData的时候，需要同时设置旧值和新值
     * 
     * @param columnNewList
     * @param columnOldList
     * @return
     */
    protected List<ColumnChangeData> transferColumnSetBoth(List<Column> columnNewList, List<Column> columnOldList) {
        List<ColumnChangeData> columnChangeDataList = new ArrayList<ColumnChangeData>();

        // new和old的length肯定一样的，所以这里随便选个
        for (int i = 0; i < columnNewList.size(); i++) {
            ColumnChangeData columnChangeData = new ColumnChangeData();
            Column columnNew = columnNewList.get(i);
            Column columnOld = columnOldList.get(i);
            columnChangeData.setNewValue(columnNew.getValue());
            columnChangeData.setOldValue(columnOld.getValue());
            String isPrimaryKey = columnNew.getIsKey() ? "1" : "0";
            columnChangeData.setPrimaryKey(isPrimaryKey);
            String columnType = columnNew.getMysqlType();
            if (columnType.startsWith("int")) {
                // 为数字类型
                columnChangeData.setColumnType("1");
            } else {
                // 为字符串类型
                columnChangeData.setColumnType("2");
            }
            columnChangeData.setColumnName(columnNew.getName());
            columnChangeDataList.add(columnChangeData);
        }


        return columnChangeDataList;
    }

    /**
     * 根据column对象转化得到ColumnChangeData
     * 
     * @param column
     * @param setNew true表示对new值set,false表示对old值set
     * @return
     */
    protected ColumnChangeData transferColumn(Column column, boolean setNew) {
        ColumnChangeData columnChangeData = new ColumnChangeData();
        if (setNew) {
            columnChangeData.setNewValue(column.getValue());
            columnChangeData.setOldValue("NULL");
        } else {
            columnChangeData.setNewValue("NULL");
            columnChangeData.setOldValue(column.getValue());
        }

        String isPrimaryKey = column.getIsKey() ? "1" : "0";
        columnChangeData.setPrimaryKey(isPrimaryKey);
        String columnType = column.getMysqlType();
        if (columnType.startsWith("int")) {
            // 为数字类型
            columnChangeData.setColumnType("1");
        } else {
            // 为字符串类型
            columnChangeData.setColumnType("2");
        }
        columnChangeData.setColumnName(column.getName());

        return columnChangeData;
    }

    protected void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN
                || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                    TransactionBegin begin = null;
                    try {
                        begin = TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    logger.info(transaction_format,
                        new Object[] { entry.getHeader().getLogfileName(),
                                       String.valueOf(entry.getHeader().getLogfileOffset()),
                                       String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime) });
                    logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
                } else if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
                    TransactionEnd end = null;
                    try {
                        end = TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("----------------\n");
                    logger.info(" END ----> transaction id: {}", end.getTransactionId());
                    logger.info(transaction_format,
                        new Object[] { entry.getHeader().getLogfileName(),
                                       String.valueOf(entry.getHeader().getLogfileOffset()),
                                       String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime) });
                }

                continue;
            }

            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage = null;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                EventType eventType = rowChage.getEventType();

                logger.info(row_format,
                    new Object[] { entry.getHeader().getLogfileName(),
                                   String.valueOf(entry.getHeader().getLogfileOffset()),
                                   entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), eventType,
                                   String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime) });

                if (eventType == EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info(" sql ----> " + rowChage.getSql() + SEP);
                    continue;
                }

                // 做一些数据转化，生成外部赛使用的数据
                logger.info("############### rowChangeDataList size: "+rowChage.getRowDatasList().size()+"#########\n");
                analyseEntryAndFlushToFile(entry);


                //for (RowData rowData : rowChage.getRowDatasList()) {
                //    if (eventType == EventType.DELETE) {
                //        printColumn(rowData.getBeforeColumnsList());
                //    } else if (eventType == EventType.INSERT) {
                //        printColumn(rowData.getAfterColumnsList());
                //    } else {
                //        printColumn(rowData.getAfterColumnsList());
                //    }
                //}
            }
        }
    }

    protected void printColumn(List<Column> columns) {
        for (Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString());
        }
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

}
