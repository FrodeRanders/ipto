package org.gautelis.repo.graphql2.model;

sealed interface NodeDef permits AttributeDef, UnitDef, RecordDef, OperationDef {}
