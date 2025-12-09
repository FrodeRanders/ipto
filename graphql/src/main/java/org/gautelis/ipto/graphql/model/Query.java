package org.gautelis.ipto.graphql.model;

import org.gautelis.ipto.repo.search.model.Operator;

public class Query {

    public record UnitIdentification(int tenantId, long unitId) {}

    public enum FilterOperator {
        GT   (Operator.GT),
        GEQ  (Operator.GEQ),
        EQ   (Operator.EQ),
        LEQ  (Operator.LEQ),
        LT   (Operator.LT),
        LIKE (Operator.LIKE),
        NEQ  (Operator.NEQ);

        private final Operator iptoOp;
        FilterOperator(Operator iptoOp) {
            this.iptoOp = iptoOp;
        }

        public Operator iptoOp() {
            return iptoOp;
        }
    }

    public record AttributeExpression(String attr, FilterOperator op, String value) {}

    public enum Logical {
        AND  (Operator.AND),
        OR   (Operator.OR);

        private final Operator iptoOp;
        Logical(Operator iptoOp) {
            this.iptoOp = iptoOp;
        }

        public Operator iptoOp() {
            return iptoOp;
        }
    }

    public record TreeExpression(Logical op, Node left, Node right) {}

    public record Node(AttributeExpression attrExpr, TreeExpression treeExpr) {}

    public record Filter(int tenantId, Node where, int offset, int size) {}
}
