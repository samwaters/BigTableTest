package sam.bigtable;

import java.util.ArrayList;

/**
 * Main class to parse parameters and start threads
 * @author Sam Waters <sam@samwaters.com>
 * @date 20-05-2015
 */
public class BigtableTest
{
	/**
	 * Main method to parse arguments and determine what to do
	 * @param args Arguments passed to application
	 */
    public static void main(String[] args)
    {
    	//Uncomment this to use log4j
    	//BasicConfigurator.configure();
    	if(args.length < 2)
    	{
    		System.out.println("Usage: hbase.jar <mode> <threads>");
    		System.out.println("<mode> should be either get or put");
    		System.out.println("<threads> should be one or more");
    		return;
    	}
    	//Make sure we have a valid operation
    	if(!args[0].equals("get") && !args[0].equals("put"))
    	{
    		System.out.println("Invalid mode - please use either get or put");
    		return;
    	}
    	int threadCount = Integer.parseInt(args[1]);
    	//Store a list of all the threads so we can shut them down later
    	ArrayList<BaseThread> threads = new ArrayList<BaseThread>();
    	if(args[0].equals("get"))
    	{
    		//Start Read threads
    		for(int i=0; i<threadCount; i++)
    		{
    			GetThread t = new GetThread("thread-" + i, "test");
    			threads.add(t);
    			t.start();
    		}
    	}
    	else
    	{
    		//Start Write threads
    		for(int i=0; i<threadCount; i++)
    		{
    			PutThread t = new PutThread("thread-" + i, "test");
    			threads.add(t);
    			t.start();
    		}
    	}
    	//Run them for 30 seconds
    	long endTime = System.currentTimeMillis() + 30000;
		while(System.currentTimeMillis() < endTime)
		{
			try
			{
				//We can sleep since the work is being done by the threads
				Thread.sleep(1000);
			}
			catch(InterruptedException e)
			{ }
		}
		//Done, shut down each of the threads cleanly
		for(int i=0; i<threads.size(); i++)
		{
			threads.get(i).shutdown(false);
		}
    }
}
