package org.gautelis.ipto.bootstrap;

import org.gautelis.ipto.graphql.configuration.OperationsWireParameters;

@FunctionalInterface
public interface IptoOperationsWiring {
    void wire(OperationsWireParameters parameters);
}
