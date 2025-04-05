package com.zyc.magic_mirror.ship.disruptor;

import com.zyc.magic_mirror.common.util.DAG;

import java.util.Map;
import java.util.Set;

public class NotOperateImpl implements Operate {

    /**
     * 和and同样的处理, 上游所有任务执行成功可触发,任意的上游失败,触发直接失败,其他继续等待
     *
     * 返回值, error: 当前节点置为失败, wait: 当前节点等待, create: 当前节点可执行
     * @param strategyId
     * @param dag
     * @param runPath
     * @return
     */
    @Override
    public String execute(String strategyId, DAG dag, Map<String, String> runPath) {
        try{
            Set<String> parents = dag.getParent(strategyId);
            String flag = ShipConst.STATUS_CREATE;
            for(String parent: parents){
                if(runPath.getOrDefault(parent, "").equalsIgnoreCase(ShipConst.STATUS_ERROR)){
                    return ShipConst.STATUS_ERROR;
                }
                if(!runPath.containsKey(parent)){
                    flag = ShipConst.STATUS_WAIT;
                }
            }
            return flag;
        }catch (Exception e){
            return ShipConst.STATUS_ERROR;
        }

    }
}
