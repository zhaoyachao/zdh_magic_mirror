package com.zyc.magic_mirror.ship.antlr4;

import com.google.common.collect.Sets;
import org.antlr.v4.runtime.tree.ParseTreeProperty;

import java.util.HashSet;
import java.util.Map;

public class ShipSRListener extends SRBaseListener{

    public ParseTreeProperty<HashSet<String>> ex= new ParseTreeProperty<>();
    /**
     * 每个标签对应的结果集必须提前输入
     */
    public Map<String, HashSet<String>> input;

    public HashSet<String> res;


    public ShipSRListener(Map<String, HashSet<String>> input){
        this.input=input;
    }

    /**
     * 开始结束
     * @param ctx
     */
    @Override
    public void exitProg(SRParser.ProgContext ctx) {
        HashSet r = getByExpr(ctx.getText());
        ex.put(ctx, r);
        super.exitProg(ctx);
    }


    /**
     * 操作符
     * @param ctx
     */
    @Override
    public void exitOPERATOR(SRParser.OPERATORContext ctx) {

        String current = ctx.getText();
        String left=ctx.expr(0).getText();
        String right=ctx.expr(1).getText();

        String operator = ctx.OPERATOR().getText();

        HashSet<String> leftObject = getByExpr(left);
        HashSet<String> rightObject = getByExpr(right);
        HashSet<String> r = Sets.newHashSet();
        if(operator.equalsIgnoreCase("&&")){
            r = and(leftObject, rightObject);
        }else if(operator.equalsIgnoreCase("||")){
            r = or(leftObject, rightObject);
        }else if(operator.equalsIgnoreCase("!")){
            r = exclude(leftObject, rightObject);
        }
        input.put(current, Sets.newHashSet(r));
        ex.put(ctx, Sets.newHashSet(r));

        super.exitOPERATOR(ctx);
    }

    private HashSet<String> getByExpr(String expr){
        if(input.containsKey(expr)){
            return input.get(expr);
        }
        String expr2 = addBracket(expr);
        if(input.containsKey(expr2)){
            return input.get(expr2);
        }
        return null;
    }

    private String addBracket(String str){
        if(!str.startsWith("(")){
            str = "("+str;
        }
        if(!str.endsWith(")")){
            str = str+")";
        }
        return str;
    }

    private HashSet and(HashSet<String> left,HashSet<String> right){
        return Sets.newHashSet(Sets.intersection(left,right));
    }
    private HashSet or(HashSet<String> left,HashSet<String> right){
        return Sets.newHashSet(Sets.union(left,right));
    }
    private HashSet exclude(HashSet<String> left,HashSet<String> right){
        return Sets.newHashSet(Sets.difference(left,right));
    }

    /**
     * 标签标识
     * @param ctx
     */
    @Override
    public void exitSTR(SRParser.STRContext ctx) {
        String label = ctx.getText();
        HashSet<String> r = getByExpr(label);
        ex.put(ctx, Sets.newHashSet(r));
        super.exitSTR(ctx);
    }

    @Override
    public void exitPARENGRP(SRParser.PARENGRPContext ctx) {
        super.exitPARENGRP(ctx);
    }
}
