/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package scribe.thrift;

import com.facebook.fb303.fb_status;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * A class to extend in ruby. We will simply implement "action"
 * Of course we can always implement Scribe.Iface directly
 * but I find it easier to use as much native java as possible
 * @author smackware
 */
public class ActionScribeHandler implements Scribe.Iface {
    int inFlightMsgs;
    Logger logger = LoggerFactory.getLogger(this.getClass().toString());
    int MAX_IN_FLIGHT_MSGS;
    fb_status status;

    public ActionScribeHandler(int MAX_IN_FLIGHT_MSGS) {
        this.MAX_IN_FLIGHT_MSGS = MAX_IN_FLIGHT_MSGS;
        this.inFlightMsgs = 0;
        this.status = fb_status.ALIVE;
    }

    public ActionScribeHandler() {
        this(10000);
    }

    public void action(List<LogEntry> messages) {
        throw new UnsupportedOperationException();
    }
    
    public ResultCode Log(List<LogEntry> messages) throws TException {
        if (messages.isEmpty()) {
            return ResultCode.OK;
        }

        // Throttling. we don't care about race conditions, it's quite all right to miss our target
        if (this.getStatus().equals(fb_status.STOPPING)) {
            logger.warn("Rejecting messages, server is stopping");
            return ResultCode.TRY_LATER;
        }

        if (inFlightMsgs > MAX_IN_FLIGHT_MSGS) {
            logger.warn("Throttling, too many messages in flight", inFlightMsgs);
            return ResultCode.TRY_LATER;
        }
        long before = System.currentTimeMillis();
        logger.debug(before + " Processing: " + messages.size());
        this.inFlightMsgs += messages.size();
        try {
            this.action(messages);
        } catch (Exception e) {
            logger.error("Caught exception in messages handler", e);
            return ResultCode.TRY_LATER;
        } finally {
            this.inFlightMsgs -= messages.size();
        }
        long delta = System.currentTimeMillis() - before;
        logger.debug(before + " Done processing: " + messages.size() + ": " + delta + "ms");
        return ResultCode.OK;
    }

    public String getName() throws TException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public String getVersion() throws TException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public fb_status getStatus() throws TException {
        return status;
    }

    public String getStatusDetails() throws TException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public Map<String, Long> getCounters() throws TException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public long getCounter(String string) throws TException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void setOption(String string, String string1) throws TException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public String getOption(String string) throws TException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public Map<String, String> getOptions() throws TException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public String getCpuProfile(int i) throws TException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public long aliveSince() throws TException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void reinitialize() throws TException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void shutdown() throws TException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public void drain() {
        logger.warn("Stopping handler");
        status = fb_status.STOPPING;
        for (int i=0; i < 60; i++) {
            if (this.inFlightMsgs == 0) {
                return;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
        logger.warn("Timed out waiting for all events to be sent");
    }
    
}
