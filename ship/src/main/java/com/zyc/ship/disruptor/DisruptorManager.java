package com.zyc.ship.disruptor;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

public class DisruptorManager {

    private static Map<String, Disruptor> manager = new ConcurrentHashMap<>();

    private static String disruptor_lock = "disruptor_lock";

    public static <T> Disruptor<T> getDisruptor(String name, int size, WorkHandler<T> workHandler) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        if(manager.containsKey(name)){
            return manager.get(name);
        }

        synchronized (disruptor_lock.intern()){
            if(!manager.containsKey(name)){
                Disruptor disruptor = init(size, workHandler);
                manager.put(name, disruptor);
            }
        }
        return manager.get(name);
    }

    public static <T> Disruptor<T> init(int size, WorkHandler<T> workHandler) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        ShipEventFactory disruptorEventFactory = new ShipEventFactory();
        int ringBufferSize = 1024 * 1024;

        Disruptor<T> disruptor = new Disruptor<T>(disruptorEventFactory, ringBufferSize, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                return thread;
            }
        }, ProducerType.MULTI, new YieldingWaitStrategy());


        WorkHandler[] workHandlers = new WorkHandler[size];
        for (int i=0;i<size;i++){
            WorkHandler handler = workHandler.getClass().getDeclaredConstructor(String.class).newInstance(i+"");
            workHandlers[i] = handler;
        }
        disruptor.handleEventsWithWorkerPool(workHandlers);
        disruptor.start();
        return disruptor;
    }

    /**
     * 动态生成disruptor入参
     * @param shipEvent
     * @return
     */
    public static EventTranslator<ShipEvent> buildByShipEvent(ShipEvent shipEvent){
        return new EventTranslator<ShipEvent>() {
            @Override
            public void translateTo(ShipEvent event, long sequence) {
                event.setWorkerDisruptor(shipEvent.getWorkerDisruptor());
                event.setMasterDisruptor(shipEvent.getMasterDisruptor());
                event.setDag(shipEvent.getDag());
                event.setStopFlag(shipEvent.getStopFlag());
                event.setStatus(shipEvent.getStatus());
                event.setRunPath(shipEvent.getRunPath());
                event.setStrategyInstance(shipEvent.getStrategyInstance());
                event.setStrategyInstanceId(shipEvent.getStrategyInstanceId());
                event.setStrategyInstanceMap(shipEvent.getStrategyInstanceMap());
                event.setShipResultMap(shipEvent.getShipResultMap());
                event.setLogId(shipEvent.getLogId());
                event.setLogGroupId(shipEvent.getLogGroupId());
                event.setCdl(shipEvent.getCdl());
                event.setGroupCdl(shipEvent.getGroupCdl());
                event.setInputParam(shipEvent.getInputParam());
                event.setShipExecutor(shipEvent.getShipExecutor());
                event.setLabelValues(shipEvent.getLabelValues());
                event.setFilterValues(shipEvent.getFilterValues());

            }
        };
    }
}
