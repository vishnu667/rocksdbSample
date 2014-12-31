package org.example;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.RestExpress;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 The given code block contains a simple counter using rocksdb's merge operator
 
 the Counters Fails for the following Cases
 
 $ ab -r -k -n 10000 -c 1000 http://localhost:9009/increment

 $ curl http://localhost:9009/get                                                                                                                                                           

 Current RocksDB Count is 137   (Expected value is 10000)
 Current AtomicLong Count is 10000

 $ curl http://localhost:9009/reset

 Current RocksDB Count is 0
 Current AtomicLong Count is 0

 $ ab -r -k -n 10000 -c 1000 http://localhost:9009/batchIncrement

 $ curl http://localhost:9009/get

 Current RocksDB Count is 16   (Expected value is 10000)
 Current AtomicLong Count is 10000
 
 */
public class RocksDBConnector {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBConnector.class);
    private  static  RocksDB db;
    private static List<String> colFamily= new ArrayList<String>();
    private static List<ColumnFamilyHandle> colFamilyHandles= new ArrayList<ColumnFamilyHandle>();
    private final static byte[] KEY = "testKey".getBytes();
    private final static byte[] BYTES_1 = util.toBytes(1L);
    private final static byte[] BYTES_0 = util.toBytes(0L);
    private  RocksDBConnector(){}
    private static final String SERVICE_NAME = "Admin Service";
    private static final int DEFAULT_EXECUTOR_THREAD_POOL_SIZE = 2;
    private static final int SERVER_PORT = 9009;
    private final static RocksDBConnector instance = new RocksDBConnector();
    private static AtomicLong simpleCounter = new AtomicLong();
    private static AtomicLong batchCounter = new AtomicLong();
    public static void main(String[] args) {
        RestExpress server = null;
        try {
            initializeRocksDb();
            server = initializeServer(args);
            server.awaitShutdown();
        } catch (IOException e) {
            LOG.info(e.getMessage());
        } catch (RocksDBException e) {
            LOG.info(e.getMessage());
        }
    }
        
    private static RestExpress initializeServer(String[] args) throws IOException {
        RestExpress server = new RestExpress()
                .setName(SERVICE_NAME)
                .setBaseUrl("http://localhost:" + SERVER_PORT)
                .setExecutorThreadCount(DEFAULT_EXECUTOR_THREAD_POOL_SIZE);

        server.uri("/increment",instance).action("incrementCounter", HttpMethod.GET).noSerialization();
        server.uri("/batchIncrement",instance).action("batchIncrementCounter", HttpMethod.GET).noSerialization();
        server.uri("/reset",instance).action("resetCounterValue", HttpMethod.GET).noSerialization();
        server.uri("/get",instance).action("getCounterValue", HttpMethod.GET).noSerialization();
        server.bind(SERVER_PORT);
        return server;
    }
    private  static void initializeRocksDb() throws RocksDBException {
            colFamily.add("default");
            RocksDB.loadLibrary();
            Options options = new Options().setCreateIfMissing(true);
            options.setMergeOperatorName("uint64add");
            options.setMaxBackgroundFlushes(1);
            options.setWriteBufferSize(50L);
            
            options.setCreateMissingColumnFamilies(true);
            if (db == null) {
                db = RocksDB.open(options, "/opt/rocksdb/data/testdata", colFamily, colFamilyHandles);
            }
    }

    private static Long getCount(){
        try {
            return util.toLong(db.get(KEY));
        } catch (RocksDBException e) {
            LOG.debug(e.getMessage());
            return 0L;
        }
    }
    private  static void resetCounter(){
        try {
            db.put(colFamilyHandles.get(0), KEY, BYTES_0);
            
        } catch (RocksDBException e) {
            LOG.debug(e.getMessage());
        }
    }
    private  static void mergeOperaton(){
        try {
            db.merge(colFamilyHandles.get(0),KEY, BYTES_1);
            simpleCounter.incrementAndGet();
        } catch (RocksDBException e) {
            LOG.debug(e.getMessage());
        }
    }
    private static void mergeBatchOperation(){
        WriteBatch batch = new WriteBatch();
        WriteOptions write_option = new WriteOptions();
        try {
            batch.merge(colFamilyHandles.get(0), KEY, BYTES_1);
            db.write(write_option,batch);
            simpleCounter.incrementAndGet();
        }catch (Exception e){
            LOG.debug(e.getMessage());
        }
    }

    public void incrementCounter(Request request, Response response) {
        mergeOperaton();
    }
    public void batchIncrementCounter(Request request, Response response) {
        mergeBatchOperation();
    }
    public void getCounterValue(Request request, Response response) {
            response.setBody("Current RocksDB Count is "+getCount()+"\n"+"Current AtomicLong Count is "+simpleCounter.get());
    }
    public void resetCounterValue(Request request, Response response) {
        simpleCounter.set(0L);
        resetCounter();
        response.setBody("Current RocksDB Count is "+getCount()+"\n"+"Current AtomicLong Count is "+simpleCounter.get());
    }
}