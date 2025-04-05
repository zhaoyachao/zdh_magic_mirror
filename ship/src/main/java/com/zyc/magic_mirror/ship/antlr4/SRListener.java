// Generated from F:/代码库/customer/ship/src/main/java/com/zyc/ship/antlr4\SR.g4 by ANTLR 4.9.2
package com.zyc.magic_mirror.ship.antlr4;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SRParser}.
 */
public interface SRListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link SRParser#prog}.
	 * @param ctx the parse tree
	 */
	void enterProg(SRParser.ProgContext ctx);
	/**
	 * Exit a parse tree produced by {@link SRParser#prog}.
	 * @param ctx the parse tree
	 */
	void exitProg(SRParser.ProgContext ctx);
	/**
	 * Enter a parse tree produced by the {@code assign}
	 * labeled alternative in {@link SRParser#stat}.
	 * @param ctx the parse tree
	 */
	void enterAssign(SRParser.AssignContext ctx);
	/**
	 * Exit a parse tree produced by the {@code assign}
	 * labeled alternative in {@link SRParser#stat}.
	 * @param ctx the parse tree
	 */
	void exitAssign(SRParser.AssignContext ctx);
	/**
	 * Enter a parse tree produced by the {@code STR}
	 * labeled alternative in {@link SRParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterSTR(SRParser.STRContext ctx);
	/**
	 * Exit a parse tree produced by the {@code STR}
	 * labeled alternative in {@link SRParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitSTR(SRParser.STRContext ctx);
	/**
	 * Enter a parse tree produced by the {@code OPERATOR}
	 * labeled alternative in {@link SRParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterOPERATOR(SRParser.OPERATORContext ctx);
	/**
	 * Exit a parse tree produced by the {@code OPERATOR}
	 * labeled alternative in {@link SRParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitOPERATOR(SRParser.OPERATORContext ctx);
	/**
	 * Enter a parse tree produced by the {@code PARENGRP}
	 * labeled alternative in {@link SRParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterPARENGRP(SRParser.PARENGRPContext ctx);
	/**
	 * Exit a parse tree produced by the {@code PARENGRP}
	 * labeled alternative in {@link SRParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitPARENGRP(SRParser.PARENGRPContext ctx);
}