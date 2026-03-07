---------------------------------------------------------------
-- Copyright (C) 2025-2026 Frode Randers
-- All rights reserved
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
---------------------------------------------------------------

INSERT INTO repo_tenant (tenantid, name, description)
VALUES (
  0, 'MALLAR', 'Mallar'
),(
  1, 'SCRATCH', 'Lekstuga'
)
;

--
-- Root units (tree tops) per tenant.
--
INSERT INTO repo_unit_kernel (tenantid, unitid, corrid, status, lastver)
VALUES (
  0, 0, '00000000-0000-0000-0000-000000000001', 30, 1
),(
  1, 0, '00000000-0000-0000-0000-000000000002', 30, 1
)
;

INSERT INTO repo_unit_version (tenantid, unitid, unitver, unitname)
VALUES (
  0, 0, 1, 'MALLAR'
),(
  1, 0, 1, 'SCRATCH'
)
;

--
-- The 'http://purl.org/dc/elements/1.1/' namespace was created in 2000 for the RDF representation
-- of the fifteen-element Dublin Core and has been widely used in data for more than twenty years.
-- This namespace corresponds to the original scope of ISO 15836, which was published first in
-- 2003 and last revised in 2017 as ISO 15836-1:2017 [ISO 15836-1:2017.
--
-- The 'http://purl.org/dc/terms/' namespace was originally created in 2001 for identifying new
-- terms coined outside of the original fifteen-element Dublin Core. In 2008, in the context
-- of defining formal semantic constraints for DCMI metadata terms in support of RDF applications,
-- the original fifteen elements themselves were mirrored in the /terms/ namespace.
--
-- As a result, there exists both a dce:date (http://purl.org/dc/elements/1.1/date) with no
-- formal range and a corresponding dcterms:date (http://purl.org/dc/terms/date) with a
-- formal range of "literal".
--
-- DCMI gently encourages use of the /terms/ namespace
--
INSERT INTO repo_namespace (alias, namespace)
VALUES (
    'dce', 'http://purl.org/dc/elements/1.1/'
), (
    'dcterms', 'http://purl.org/dc/terms/'
), (
    'foaf', 'http://xmlns.com/foaf/0.1/'
), (
    'dcat', 'http://www.w3.org/ns/dcat#'
), (
    'prov', 'http://www.w3.org/ns/prov#'
), (
    'dcatap', 'http://data.europa.eu/r5r/'
), (
    'adms', 'http://www.w3.org/ns/adms#'
), (
    'schema', 'http://schema.org/'
), (
    'spdx', 'http://spdx.org/rdf/terms#'
), (
    'org', 'https://www.w3.org/ns/org#'
), (
    'owl', 'http://www.w3.org/2002/07/owl#'
), (
    'rdf', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
), (
    'vcard', 'http://www.w3.org/2006/vcard/ns#'
), (
    'locn', 'http://www.w3.org/ns/locn#'
), (
    'odrs', 'http://schema.theodi.org/odrs#'
)
;
