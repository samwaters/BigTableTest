package sam.bigtable;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class BaseThread extends Thread
{
	protected boolean canRun;
	protected Connection connection;
	protected Table table;
	protected String threadName;
	
	public BaseThread()
	{
		
	}
	
	public BaseThread(String threadName, String tableName)
	{
		try
		{
			this.canRun = true;
			OperationTimer cTimer = new OperationTimer();
			System.out.println("Thread " + threadName + " connecting...");
			cTimer.startTiming();
			this.connection  = ConnectionFactory.createConnection();
			cTimer.stopTiming();
			System.out.println("Connected in " + cTimer.getTotalTime() + "ms");
			this.table = this.connection.getTable(TableName.valueOf(tableName));
		}
		catch(IOException e)
		{
			System.out.println("Thread " + threadName + " throwing exception: " + e.getMessage());
			this.canRun = false;
		}
		this.threadName = threadName;
	}
	
	protected void closeConnections()
	{
		try
		{
			this.connection.close();
		}
		catch (IOException e) {}
	}
	
	public void shutdown(boolean forceClose)
	{
		this.canRun = false;
		if(forceClose)
		{
			this.closeConnections();
		}
	}
}
