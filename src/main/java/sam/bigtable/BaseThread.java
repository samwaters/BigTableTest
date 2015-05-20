package sam.bigtable;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

/**
 * Base thread to support read and write threads, containing connect and disconnect logic
 * @author Sam Waters <sam@samwaters.com>
 * @date 20-05-2015
 */
public class BaseThread extends Thread
{
	protected boolean canRun;
	protected Connection connection;
	protected Table table;
	protected String threadName;
	
	/**
	 * Constructor to allow extending
	 */
	public BaseThread()
	{
		
	}
	
	/**
	 * Set up the connection to BigTable
	 * hbase-site.xml must be accessible for this to work (normally conf/hbase-site.xml)
	 * @param threadName Unique name for this thread
	 * @param tableName Name of the table to use
	 */
	public BaseThread(String threadName, String tableName)
	{
		try
		{
			this.canRun = true;
			OperationTimer cTimer = new OperationTimer();
			System.out.println("Thread " + threadName + " connecting...");
			cTimer.startTiming();
			//Connect to BigTable
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
	
	/**
	 * Disconnect from BigTable cleanly
	 */
	protected void closeConnections()
	{
		try
		{
			this.connection.close();
		}
		catch (IOException e) {}
	}
	
	/**
	 * Shut down cleanly
	 * @param forceClose Whether to disconnect now
	 */
	public void shutdown(boolean forceClose)
	{
		this.canRun = false;
		if(forceClose)
		{
			this.closeConnections();
		}
	}
}
