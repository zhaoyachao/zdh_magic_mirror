package com.zyc.ship.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zyc.common.util.DAG;
import com.zyc.common.util.JsonUtil;
import org.junit.Test;

import java.util.*;


public class DAGTest {

    @Test
    public void addEdge() {
        DAG dag=new DAG();
        dag.addEdge("A","A1");
        dag.addEdge("A","A2");
        dag.addEdge("B","A1");
        dag.addEdge("B","B1");
        dag.addEdge("A1","A3");
        dag.addEdge("A2","A4");
        dag.addEdge("B1","A4");
        dag.addEdge("A3","A5");
        dag.addEdge("A4","A5");
        //dag 转 antlr4表达式

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
                if(children==null)
                    break;
                System.out.println("main children: "+children);
                String str = checkParentInRunPath(children, dag.getParent(children),dag,runPath);
                System.out.println("main: "+str);
                Set  tmp = dag.getChildren(children);
                if(tmp != null && tmp.size()>0){
                    queue.addAll(tmp);
                }
            }
            //判断子节点的父节点是否都在runPath中
        }

        System.out.println("最终："+JsonUtil.formatJsonString(runPath));
    }

    @Test
    public void checkChildren(){

        String str = " [{\n" +
                "\t\t\"from\": \"1165601597590343680\",\n" +
                "\t\t\"to\": \"1165601597602926592\"\n" +
                "\t}, {\n" +
                "\t\t\"from\": \"1165601597615509504\",\n" +
                "\t\t\"to\": \"1165601597590343680\"\n" +
                "\t}, {\n" +
                "\t\t\"from\": \"1165601597602926592\",\n" +
                "\t\t\"to\": \"1165601597628092416\"\n" +
                "\t}, {\n" +
                "\t\t\"from\": \"1165601597615509504\",\n" +
                "\t\t\"to\": \"1165601597636481024\"\n" +
                "\t}, {\n" +
                "\t\t\"from\": \"1165601597590343680\",\n" +
                "\t\t\"to\": \"1165601597649063936\"\n" +
                "\t}]";

        JSONArray jsonArray = JSON.parseArray(str);
        DAG dag = new DAG();
        for(Object o: jsonArray){
            String from = ((JSONObject)o).getString("from");
            String to = ((JSONObject)o).getString("to");
            dag.addEdge(from, to);
        }

        System.out.println(dag.getAllChildren("1165601597615509504"));

    }

    public String checkParentInRunPath(String current, Set<String> parents, DAG dag, Map<String,String> runPath){
        System.out.println("current: "+current+" ,parents: "+ JsonUtil.formatJsonString(parents));
        String str = "";
        //无父节点,返回当前节点的配置信息,此处暂时已节点代替
        if(parents == null || parents.size()==0){
            System.out.println("current: "+current+" ,parents 为空: "+ JsonUtil.formatJsonString(parents));
            return current;
        }
        for (String parent:parents){
            if(!runPath.keySet().contains(parent)){
                String config = checkParentInRunPath(parent, dag.getParent(parent),dag,runPath);
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
        String currentType = getCurrentType(current);
        String operate = getCurrentOperate(current);

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

    public String getCurrentType(String current){
        Map<String,String> map=new HashMap<>();
        map.put("A","Label");
        map.put("B","Label");
        map.put("A1","operate");
        map.put("A2","Label");
        map.put("B1","Label");
        map.put("A3","Label");
        map.put("A4","Label");
        map.put("A5","operate");

        return map.get(current);
    }

    public String getCurrentOperate(String current){
        Map<String,String> map=new HashMap<>();
        map.put("A","And");
        map.put("B","And");
        map.put("A1","And");
        map.put("A2","or");
        map.put("B1","And");
        map.put("A3","And");
        map.put("A4","and");
        map.put("A5","or");

        return map.get(current);
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
}
