/*
 * Copyright (C) 2025 Frode Randers
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
package org.gautelis.repo.search.query;

import org.gautelis.repo.search.model.AttributeSearchItem;
import org.gautelis.repo.search.model.SearchItem;
import org.gautelis.repo.search.model.UnitSearchItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.gautelis.repo.db.Column.*;
import static org.gautelis.repo.db.Table.*;
import static org.gautelis.repo.search.model.Operator.EQ;

public final class LeafExpression<T extends SearchItem<?>> implements SearchExpression {
    protected static final Logger log = LoggerFactory.getLogger(LeafExpression.class);

    final T item;
    private String reference = null;


    public LeafExpression(T item) {
        Objects.requireNonNull(item, "item");
        this.item = item;
    }

    public T getItem() {
        return item;
    }

    public void setReference(String reference) {
        this.reference = reference;
    }

    public Optional<String> getReference() {
        return Optional.ofNullable(reference);
    }

    @Override
    public String toSql(
            boolean usePrepare,
            Map<String, SearchItem<?>> commonConstraintValues
    ) {
        StringBuilder sb = new StringBuilder();
        if (item instanceof AttributeSearchItem<?> asi) {
            attributeConstraint(sb, reference, asi, usePrepare, commonConstraintValues);
        }
        else if (item instanceof UnitSearchItem<?> usi) {
            unitConstraint(sb, usi, usePrepare);
        }
        // else AssociationSearchItem ...  TODO
        return sb.toString();
    }

    private void unitConstraint(
            StringBuilder sb,
            UnitSearchItem<?> item,
            boolean usePrepare
    ) {
        sb.append(item.getColumn()).append(" ");
        sb.append(item.getOperator());
        sb.append(" ");
        if (usePrepare) {
            sb.append("?");
        } else {
            switch (item.getType()) {
                case TIME -> sb.append("TIMESTAMP '").append(item.getValue()).append("'");
                default -> sb.append(item.getValue());
            }
        }
    }

    private void attributeConstraint(
            StringBuilder sb,
            String reference,
            AttributeSearchItem<?> item,
            boolean usePrepare,
            Map<String, SearchItem<?>> commonConstraintValues
    ) {
        sb.append(reference).append(" AS (");
        sb.append("SELECT av.tenantid, av.unitid ");
        sb.append("FROM repo_attribute_value av ");
        sb.append("JOIN ");
        switch(item.getType()) {
            case STRING ->  sb.append(ATTRIBUTE_STRING_VALUE_VECTOR);
            case TIME ->    sb.append(ATTRIBUTE_TIME_VALUE_VECTOR);
            case INTEGER -> sb.append(ATTRIBUTE_INTEGER_VALUE_VECTOR);
            case LONG ->    sb.append(ATTRIBUTE_LONG_VALUE_VECTOR);
            case DOUBLE ->  sb.append(ATTRIBUTE_DOUBLE_VALUE_VECTOR);
            case BOOLEAN -> sb.append(ATTRIBUTE_BOOLEAN_VALUE_VECTOR);
        }
        sb.append(" ON ").append(ATTRIBUTE_VALUE_VALUEID).append(" ");
        sb.append(EQ);
        sb.append(" ").append(ATTRIBUTE_VALUE_VECTOR_VALUEID).append(" ");
        sb.append("WHERE ");
        if (commonConstraintValues.containsKey(UNIT_TENANTID.toString())) {
            //--------------------------------------------------------------------
            // Has *UNIT*_TENANTID specified (ut.tenantid), so we constrain
            // ATTRIBUTE_VERSION_TENANTID accordingly!!
            //--------------------------------------------------------------------
            SearchItem<?> unitItem = commonConstraintValues.get(UNIT_TENANTID.toString());
            if (usePrepare) {
                sb.append(ATTRIBUTE_VALUE_TENANTID).append(" ").append(unitItem.getOperator()).append(" ? AND ");
            } else {
                sb.append(ATTRIBUTE_VALUE_TENANTID).append(" ").append(unitItem.getOperator()).append(" ");
                sb.append(commonConstraintValues.get(UNIT_TENANTID.toString()).getValue());
                sb.append(" AND ");
            }
        }
        sb.append(ATTRIBUTE_VALUE_ATTRID).append(" = ").append(item.getAttrId()).append(" ");
        switch(item.getType()) {
            case STRING -> {
                sb.append("AND lower(").append(ATTRIBUTE_VALUE_VECTOR_ENTRY).append(") ");
                sb.append(item.getOperator());
                if (usePrepare) {
                    sb.append(" lower(?)");
                } else {
                    sb.append(" lower('").append(item.getValue()).append("')");
                }
            }
            case TIME -> {
                sb.append("AND ").append(ATTRIBUTE_VALUE_VECTOR_ENTRY).append(" ");
                sb.append(item.getOperator());
                if (usePrepare) {
                    sb.append(" ?");
                } else {
                    sb.append(" TIMESTAMP '").append(item.getValue()).append("'");
                }
            }
            default -> {
                sb.append("AND ").append(ATTRIBUTE_VALUE_VECTOR_ENTRY).append(" ");
                sb.append(item.getOperator());
                if (usePrepare) {
                    sb.append(" ?");
                } else {
                    sb.append(" ").append(item.getValue());
                }
            }
        }
        sb.append(")");
    }
}
