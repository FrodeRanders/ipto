grammar SearchExpression;

query
    : expr EOF
    ;

expr
    : orExpr
    ;

orExpr
    : andExpr (OR andExpr)*
    ;

andExpr
    : notExpr (AND notExpr)*
    ;

notExpr
    : NOT notExpr
    | primary
    ;

primary
    : LPAREN expr RPAREN
    | predicate
    ;

predicate
    : field operator value
    ;

field
    : ATTR_NAME
    | IDENT
    ;

operator
    : EQ
    | NEQ
    | LT
    | LE
    | GT
    | GE
    | LIKE
    ;

value
    : STRING
    | NUMBER
    | TRUE
    | FALSE
    | IDENT
    ;

LPAREN : '(';
RPAREN : ')';

AND  : A N D;
OR   : O R;
NOT  : N O T;
LIKE : L I K E;

TRUE  : T R U E;
FALSE : F A L S E;

GE  : '>=';
LE  : '<=';
NEQ : '<>' | '!=';
EQ  : '=' | '==';
GT  : '>';
LT  : '<';

ATTR_NAME
    : NAME_PREFIX ':' NAME_LOCAL
    | ':' NAME_LOCAL
    ;

IDENT
    : [A-Za-z_][A-Za-z0-9_]*
    ;

NUMBER
    : '-'? DIGIT+ ('.' DIGIT+)?
    ;

STRING
    : '"' ( '\\' . | ~["\\] )* '"'
    ;

WS : [ \t\r\n]+ -> skip;

fragment NAME_PREFIX : [A-Za-z][A-Za-z0-9_-]*;
fragment NAME_LOCAL : ~[ \t\r\n()=<>!]+;
fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
