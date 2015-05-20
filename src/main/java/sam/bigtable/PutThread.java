package sam.bigtable;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Worker thread to perform write operations on the cluster
 * @author Sam Waters <sam@samwaters.com>
 * @date 20-05-2015
 */
public class PutThread extends BaseThread
{
	/**
	 * Set up the connection to BigTable
	 * @param threadName Unique name for this thread
	 * @param tableName Table to use on the cluster
	 */
	public PutThread(String threadName, String tableName)
	{
		//Connection logic is handled by BaseThread
		super(threadName, tableName);
	}
	
	/**
	 * Run the operations until told otherwise
	 * This thread will be told to shut down after 30 seconds by the main thread
	 */
	public void run()
	{
		int operationCount = 0;
		//Print status every 5 seconds
		long nextPrintTime = System.currentTimeMillis() + 5000;
		Random r = new Random();
		while(this.canRun)
		{
			//Row key is chosen at random
			String rowKey = this.threadName + "-row-" + r.nextInt(1000000);
			//Build up a Put request
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col-1"), Bytes.toBytes("Value 1"));
			put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col-2"), Bytes.toBytes("Value 2"));
			put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col-3"), Bytes.toBytes("Value 3"));
			try
			{
				//Perform write operation
				this.table.put(put);
			}
			catch(IOException e) {}
			operationCount++;
			if(System.currentTimeMillis() >= nextPrintTime)
			{
				//Print status
				System.out.println(this.threadName + " : " + (operationCount / 5) + " writes/s");
				operationCount = 0;
				nextPrintTime = System.currentTimeMillis() + 5000; 
			}
		}
		//Done, shut down cleanly
		this.closeConnections();
	}
}
