package com.zyc.magic_mirror.ship.antlr4;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.HashSet;

public class ShipAnalysis {

    public void analysis(){

        //提前准备好标签和结果集

        //输入表达式
        CharStream input = CharStreams.fromString(" z&&b || c");
        SRLexer lexer = new SRLexer(input);

        CommonTokenStream tokens = new CommonTokenStream(lexer);

        SRParser zParser=new SRParser(tokens);

        ParseTree pt=zParser.prog();

        ShipSRListener shipSRListener=new ShipSRListener(null);
        ParseTreeWalker ptw=new ParseTreeWalker();
        ptw.walk(shipSRListener, pt);
        //结果集
        HashSet<String> res = shipSRListener.ex.get(pt);
    }

}
