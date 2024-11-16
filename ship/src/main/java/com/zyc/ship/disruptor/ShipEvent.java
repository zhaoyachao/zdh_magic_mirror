package com.zyc.ship.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import com.zyc.common.entity.StrategyInstance;
import com.zyc.common.util.DAG;
import com.zyc.ship.entity.InputParam;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 统一参数
 */
public class ShipEvent {

    private AtomicInteger sequence = new AtomicInteger(0);

    private Map<String,Object> labelValues;

    private Map<String,Object> filterValues;

    private InputParam inputParam;
    /**
     * 唯一请求id
     */
    private String requestId;

    /**
     * 策略组内日志id
     */
    private long logGroupId;

    /**
     * 策略日志id
     */
    private long logId;

    private CountDownLatch cdl;

    private CountDownLatch groupCdl;

    /**
     * create,wait,error,success
     */
    private String status;

    /**
     * 信息说明
     */
    private String msg;

    /**
     * 策略树实例ID
     */
    private String strategyGroupInstanceId;
    /**
     * 策略id
     */
    private String strategyInstanceId;

    private StrategyInstance strategyInstance;

    private Map<String, StrategyInstance> strategyInstanceMap;

    private DAG dag;

    private List<Map<String,String>> dagMap;

    /**
     * 判断是否暂停任务
     * 因超时,业务手动暂停策略导致的暂停,使用此方案实现的原因,disruptor内部线程无法物理暂停,因此使用此参数做到逻辑暂停(跳过任务处理)
     *
     * 参数flag:true 暂停任务, flag:false: 正常执行任务
     */
    private StopFlag stopFlag;

    /**
     * 记录每个策略实例(节点)的运行状态
     */
    private Map<String, String> runPath = new ConcurrentHashMap<>();

    /**
     * 返回结果
     */
    private Map<String, ShipResult> shipResultMap;

    private Disruptor masterDisruptor;

    private Disruptor workerDisruptor;

    private ShipExecutor shipExecutor;

    /**
     * 执行中产生的数据
     */
    private ConcurrentHashMap<String, Object> runParam;


    public AtomicInteger getSequence(){
        return sequence;
    }

    public void setSequence(AtomicInteger sequence){
        this.sequence = sequence;
    }

    /**
     * 获取执行顺序
     * @return
     */
    public int incrementAndGet(){
        return this.sequence.incrementAndGet();
    }

    public Map<String, Object> getLabelValues() {
        return labelValues;
    }

    public void setLabelValues(Map<String, Object> labelValues) {
        this.labelValues = labelValues;
    }

    public Map<String, Object> getFilterValues() {
        return filterValues;
    }

    public void setFilterValues(Map<String, Object> filterValues) {
        this.filterValues = filterValues;
    }

    public InputParam getInputParam() {
        return inputParam;
    }

    public void setInputParam(InputParam inputParam) {
        this.inputParam = inputParam;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public long getLogGroupId() {
        return logGroupId;
    }

    public void setLogGroupId(long logGroupId) {
        this.logGroupId = logGroupId;
    }

    public long getLogId() {
        return logId;
    }

    public void setLogId(long logId) {
        this.logId = logId;
    }

    public CountDownLatch getCdl() {
        return cdl;
    }

    public void setCdl(CountDownLatch cdl) {
        this.cdl = cdl;
    }

    public CountDownLatch getGroupCdl() {
        return groupCdl;
    }

    public void setGroupCdl(CountDownLatch groupCdl) {
        this.groupCdl = groupCdl;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getStrategyGroupInstanceId() {
        return strategyGroupInstanceId;
    }

    public void setStrategyGroupInstanceId(String strategyGroupInstanceId) {
        this.strategyGroupInstanceId = strategyGroupInstanceId;
    }

    public String getStrategyInstanceId() {
        return strategyInstanceId;
    }

    public void setStrategyInstanceId(String strategyInstanceId) {
        this.strategyInstanceId = strategyInstanceId;
    }

    public StrategyInstance getStrategyInstance() {
        return strategyInstance;
    }

    public void setStrategyInstance(StrategyInstance strategyInstance) {
        this.strategyInstance = strategyInstance;
    }

    public Map<String, StrategyInstance> getStrategyInstanceMap() {
        return strategyInstanceMap;
    }

    public void setStrategyInstanceMap(Map<String, StrategyInstance> strategyInstanceMap) {
        this.strategyInstanceMap = strategyInstanceMap;
    }

    public DAG getDag() {
        return dag;
    }

    public void setDag(DAG dag) {
        this.dag = dag;
    }

    public List<Map<String, String>> getDagMap() {
        return dagMap;
    }

    public void setDagMap(List<Map<String, String>> dagMap) {
        this.dagMap = dagMap;
    }

    public StopFlag getStopFlag() {
        return stopFlag;
    }

    public void setStopFlag(StopFlag stopFlag) {
        this.stopFlag = stopFlag;
    }

    public Map<String, String> getRunPath() {
        return runPath;
    }

    public void setRunPath(Map<String, String> runPath) {
        this.runPath = runPath;
    }

    public Map<String, ShipResult> getShipResultMap() {
        return shipResultMap;
    }

    public void setShipResultMap(Map<String, ShipResult> shipResultMap) {
        this.shipResultMap = shipResultMap;
    }

    public Disruptor getMasterDisruptor() {
        return masterDisruptor;
    }

    public void setMasterDisruptor(Disruptor masterDisruptor) {
        this.masterDisruptor = masterDisruptor;
    }

    public Disruptor getWorkerDisruptor() {
        return workerDisruptor;
    }

    public void setWorkerDisruptor(Disruptor workerDisruptor) {
        this.workerDisruptor = workerDisruptor;
    }

    public ShipExecutor getShipExecutor() {
        return shipExecutor;
    }

    public void setShipExecutor(ShipExecutor shipExecutor) {
        this.shipExecutor = shipExecutor;
    }

    public ConcurrentHashMap<String, Object> getRunParam() {
        return runParam;
    }

    public void setRunParam(ConcurrentHashMap<String, Object> runParam) {
        this.runParam = runParam;
    }

    public void clear(){
        this.setMasterDisruptor(null);
        this.setWorkerDisruptor(null);
        this.setDag(null);
        this.setStopFlag(null);
        this.setRunPath(null);
        this.setStrategyGroupInstanceId(null);
        this.setStrategyInstance(null);
        this.setStrategyInstanceId(null);
        this.setStrategyInstanceMap(null);
        this.setStatus(null);
        this.setCdl(null);
        this.setGroupCdl(null);
        this.setShipExecutor(null);
        this.setMsg(null);
    }
}
