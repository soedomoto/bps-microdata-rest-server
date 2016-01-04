package id.go.bps.microdata;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

public class MicrodataDaemon implements Daemon {	
	private Thread myThread; 
    private boolean stopped = false;

	@Override
	public void destroy() {
		myThread = null;
	}

	@Override
	public void init(DaemonContext ctx) throws DaemonInitException, Exception {
		final String[] args = ctx.getArguments(); 
	       
        myThread = new Thread(){
            @Override
            public synchronized void start() {
            	MicrodataDaemon.this.stopped = false;
                super.start();
            }

            @Override
            public void run() {            
                while(!stopped){
                    MicrodataApplication.main(args);
                }
            }
        };
	}

	@Override
	public void start() throws Exception {
		myThread.start();
	}

	@Override
	public void stop() throws Exception {
		stopped = true;
        try{
            myThread.join(1000);
        }catch(InterruptedException e){
            System.err.println(e.getMessage());
            throw e;
        }
	}

}
