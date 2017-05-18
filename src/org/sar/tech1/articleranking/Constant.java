package org.sar.tech1.articleranking;

import java.util.regex.Pattern;

/**
 * 
 * This file consists of all the constants used in this technique.
 * 
 * Identifiers are taken from the given data set as such. 
 * 
 * Seperators are what we defined for convinience to understand.
 * 
 * 
 */

public class Constant {
	
	
public	static final  Pattern LINEPATTERN=Pattern.compile("\n");
public	static final  Pattern SPACEPATTERN=Pattern.compile("\\s+");
public	static final  String CITATION_IDENTIFIER="#%";
public	static final  String AUTHOR_IDENTIFIER="#@";
public	static final  String TITLE_IDENTIFIER="#*";
public	static final  String VENUE_IDENTIFIER="#c";


public	static final String INDEGREE="INDEGREE";
public	static final String INDEX_SEPARATOR="##";
public	static final String SEPARATOR="###";
public	static final String AUTHORSEPARATOR="@@";
public	static final String VENUESEPERATOR="**";
public	static final String TABSEPERATOR="INDEGREE";

public	static final String PC_IDENTIFIER="PC";
public	static final String PV_IDENTIFIER="PV";
public	static final String PA_IDENTIFIER="PA";
public	static final String PI_IDENTIFIER="PI";


}

