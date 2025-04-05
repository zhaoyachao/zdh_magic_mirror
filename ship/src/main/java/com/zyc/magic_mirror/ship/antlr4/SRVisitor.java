// Generated from F:/代码库/customer/ship/src/main/java/com/zyc/ship/antlr4\SR.g4 by ANTLR 4.9.2
package com.zyc.magic_mirror.ship.antlr4;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link SRParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface SRVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link SRParser#prog}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitProg(SRParser.ProgContext ctx);
	/**
	 * Visit a parse tree produced by the {@code assign}
	 * labeled alternative in {@link SRParser#stat}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAssign(SRParser.AssignContext ctx);
	/**
	 * Visit a parse tree produced by the {@code STR}
	 * labeled alternative in {@link SRParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSTR(SRParser.STRContext ctx);
	/**
	 * Visit a parse tree produced by the {@code OPERATOR}
	 * labeled alternative in {@link SRParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOPERATOR(SRParser.OPERATORContext ctx);
	/**
	 * Visit a parse tree produced by the {@code PARENGRP}
	 * labeled alternative in {@link SRParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPARENGRP(SRParser.PARENGRPContext ctx);
}