--#SET TERMINATOR @

SET CURRENT SCHEMA REPO@

CALL sqlj.install_jar('file:/ipto/repo-udpf-1.0-SNAPSHOT.jar', 'IPTO_JAR', 0)@
CALL sqlj.refresh_classes()@

--#SET TERMINATOR ;
