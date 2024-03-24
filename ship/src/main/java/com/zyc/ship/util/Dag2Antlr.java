package com.zyc.ship.util;

import cn.hutool.json.JSONUtil;
import com.zyc.common.util.DAG;

import java.util.*;

public class Dag2Antlr {


    public String strategyDag2Antlr(DAG dag, Map<String,String> nodeTypes, Map<String,String> nodeOperates){
        Map<String,String> runPath = new HashMap<>();
        //1:遍历得到dag根节点
        Set<String> roots = getRoots(dag);
        //遍历根节点,记录每个
        for(String root: roots){
            //记录root具体信息
            runPath.put(root,root);

            //获取当前根节点下子节点信息
            Set<String> childrens = dag.getChildren(root);
            Queue<String> queue = new LinkedList<>();

            queue.addAll(childrens);
            while (true){
                String children = queue.poll();
                if(children==null) {
                    break;
                }
                System.out.println("main children: "+children);
                String str = checkParentInRunPath(children, dag.getParent(children),dag,runPath,nodeTypes, nodeOperates);
                System.out.println("main: "+str);
                Set  tmp = dag.getChildren(children);
                if(tmp != null && tmp.size()>0){
                    queue.addAll(tmp);
                }
            }
            //判断子节点的父节点是否都在runPath中
        }

        Set<String> outPutRoots = getOutPutRoots(dag, nodeTypes);
        System.out.println("最终："+JSONUtil.toJsonStr(runPath));
        return JSONUtil.toJsonStr(runPath);
    }

    public String checkParentInRunPath(String current, Set<String> parents, DAG dag, Map<String,String> runPath, Map<String,String> nodeTyps, Map<String,String> nodeOperates ){
        System.out.println("current: "+current+" ,parents: "+ JSONUtil.toJsonStr(parents));
        String str = "";
        //无父节点,返回当前节点的配置信息,此处暂时已节点代替
        if(parents == null || parents.size()==0){
            System.out.println("current: "+current+" ,parents 为空: "+ JSONUtil.toJsonStr(parents));
            return current;
        }
        for (String parent:parents){
            if(!runPath.keySet().contains(parent)){
                String config = checkParentInRunPath(parent, dag.getParent(parent),dag,runPath, nodeTyps, nodeOperates);
                runPath.put(parent,config);
            }else{

            }
            if(str.equalsIgnoreCase("")){
                str = "("+runPath.get(parent)+")";
            }else{
                str = str+"_("+runPath.get(parent)+")";
            }
        }
        //获取current 类型，并且获取对应操作符and, or, !
        String currentType = nodeTyps.getOrDefault(current,"lable");
        String operate = nodeOperates.getOrDefault(current,"and");

        str = str.replaceAll("_"," "+operate+" ");
        if(currentType.equalsIgnoreCase("label")){
            runPath.put(current,current+" "+operate+" ("+str+")");
            System.out.println("checkParentInRunPath: "+current+" "+operate+" ("+str+")");
        }else{
            runPath.put(current," ("+str+") ");
            System.out.println("checkParentInRunPath operate: "+current+" "+operate+" ("+str+")");
        }

        //所有的节点都满足了,拼接and操作
        return str;
    }
    private Set<String> getRoots(DAG dag){
        Set<String> roots = new HashSet<>();
        Set<String> sources = dag.getSources();

        for (String source:sources){
            //无上游节点则是根节点
            boolean hashParent = dag.hashParent(source);
            if(!hashParent){
                roots.add(source);
            }
        }

        return roots;
    }

    private Set<String> getOutPutRoots(DAG dag,Map<String,String> nodeTypes){
        Set<String> outputs = new HashSet<>();
        Set<String> sources = dag.getSources();
        for (String source:sources){
            //如果当前节点是label,且下游节点不label,则认为当前节点是输出节点
            if(nodeTypes.get(source).equalsIgnoreCase("label")){
                Set<String> childrens = dag.getChildren(source);
                for(String children:childrens){
                    if(!nodeTypes.get(children).equalsIgnoreCase("label")){
                        outputs.add(source);
                    }
                }
            }
        }
        return outputs;
    }
}
