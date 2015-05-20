package sam.bigtable;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutThread extends BaseThread
{
	public PutThread(String threadName, String tableName)
	{
		super(threadName, tableName);
	}
	
	public void run()
	{
		int operationCount = 0;
		long nextPrintTime = System.currentTimeMillis() + 5000;
		Random r = new Random();
		while(this.canRun)
		{
			String rowKey = this.threadName + "-row-" + r.nextInt(1000000);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col-1"), Bytes.toBytes("Value 1"));
			put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col-2"), Bytes.toBytes("Value 2"));
			put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col-3"), Bytes.toBytes("Value 3"));
			try
			{
				this.table.put(put);
			}
			catch(IOException e) {}
			operationCount++;
			if(System.currentTimeMillis() >= nextPrintTime)
			{
				System.out.println(this.threadName + " : " + (operationCount / 5) + " writes/s");
				operationCount = 0;
				nextPrintTime = System.currentTimeMillis() + 5000; 
			}
		}
		this.closeConnections();
	}
}
