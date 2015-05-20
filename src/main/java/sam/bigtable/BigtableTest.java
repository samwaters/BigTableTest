package sam.bigtable;

import java.util.ArrayList;

public class BigtableTest
{
    public static void main(String[] args)
    {
    	//BasicConfigurator.configure();
    	if(args.length < 2)
    	{
    		System.out.println("Usage: hbase.jar <mode> <threads>");
    		System.out.println("<mode> should be either get or put");
    		System.out.println("<threads> should be one or more");
    		return;
    	}
    	if(!args[0].equals("get") && !args[0].equals("put"))
    	{
    		System.out.println("Invalid mode - please use either get or put");
    		return;
    	}
    	int threadCount = Integer.parseInt(args[1]);
    	ArrayList<BaseThread> threads = new ArrayList<BaseThread>();
    	if(args[0].equals("get"))
    	{
    		for(int i=0; i<threadCount; i++)
    		{
    			GetThread t = new GetThread("thread-" + i, "test");
    			threads.add(t);
    			t.start();
    		}
    	}
    	else
    	{
    		for(int i=0; i<threadCount; i++)
    		{
    			PutThread t = new PutThread("thread-" + i, "test");
    			threads.add(t);
    			t.start();
    		}
    	}
    	long endTime = System.currentTimeMillis() + 30000;
		while(System.currentTimeMillis() < endTime)
		{
			try
			{
				Thread.sleep(1000);
			}
			catch(InterruptedException e)
			{ }
		}
		for(int i=0; i<threads.size(); i++)
		{
			threads.get(i).shutdown(false);
		}
    }
}
