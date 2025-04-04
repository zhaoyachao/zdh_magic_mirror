package com.zyc.magic_mirror.ship.disruptor;

import com.google.common.collect.Sets;
import com.zyc.magic_mirror.common.util.DAG;

import java.util.Map;
import java.util.Set;

public class OrOperateImpl implements Operate {

    /**
     * or, 所有上游执行完成后,触发检查, 上游任意的成功,可触发执行,上游全部失败,可触发失败,其他等待
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
            String flag = ShipConst.STATUS_WAIT;
            int errorNum = 0;
            for(String parent: parents){
                if(Sets.newHashSet(ShipConst.STATUS_SUCCESS, ShipConst.STATUS_ERROR).contains(runPath.getOrDefault(parent, "").toLowerCase())){
                    //上游存在未完成的任务
                    return flag;
                }
                if(runPath.getOrDefault(parent, "").equalsIgnoreCase(ShipConst.STATUS_SUCCESS)){
                    return ShipConst.STATUS_CREATE;
                }
                if(!runPath.containsKey(parent)){
                    flag = ShipConst.STATUS_WAIT;
                }else if(runPath.getOrDefault(parent, "").equalsIgnoreCase(ShipConst.STATUS_ERROR)){
                    errorNum = errorNum + 1;
                }
            }

            if(parents.size() == errorNum){
                flag = ShipConst.STATUS_ERROR;
            }
            return flag;
        }catch (Exception e){
            return ShipConst.STATUS_ERROR;
        }

    }
}
