--#SET TERMINATOR @

SET CURRENT SCHEMA REPO@

-- File name must match artefact by Maven coordinate used in setup-for-test.py
CALL sqlj.install_jar('file:/ipto/repo-udpf-1.0-SNAPSHOT.jar', 'IPTO_JAR', 0)@
CALL sqlj.refresh_classes()@

--#SET TERMINATOR ;
