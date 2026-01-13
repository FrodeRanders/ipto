/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    public record Filter(int tenantId, Node where, String text, int offset, int size) {}
}
