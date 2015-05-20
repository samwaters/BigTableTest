package sam.bigtable;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

public class GetThread extends BaseThread
{
	public GetThread(String threadName, String tableName)
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
			try
			{
				Result result = this.table.get(new Get(rowKey.getBytes()));
			}
			catch(IOException e) {}
			operationCount++;
			if(System.currentTimeMillis() >= nextPrintTime)
			{
				System.out.println(this.threadName + " : " + (operationCount / 5) + " reads/s");
				operationCount = 0;
				nextPrintTime = System.currentTimeMillis() + 5000; 
			}
		}
		this.closeConnections();
	}
	
	public void printResult(Result result)
	{
		for (Cell cell : result.listCells())
		{
            String row = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String column = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            long timestamp = cell.getTimestamp();
            System.out.printf("Row: " + row + ", Family: " + family + ", Column: " + column + ", Timestamp: " + timestamp + ", Value: " + value);
        }
	}
}
