grammar SR;

prog : stat;
stat: expr # assign
    ;
expr:  expr OPERATOR expr   #OPERATOR
| '('expr')'                  #PARENGRP
| STR                            #STR
;

OPERATOR: ('&&'|'||'|'!');

STR : [a-zA-Z]+ ;
WS : [ \r\n\t]+ -> skip;