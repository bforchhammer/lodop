-- Incoming Properties Co-Occurence

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

resourceObjects = FILTER quads BY (NOT predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type') AND (object.ntype == 4 OR object.ntype == 5);
resourceObjectsFlat = FOREACH resourceObjects GENERATE object.value AS resourceObject, predicate;
groups = GROUP resourceObjectsFlat BY resourceObject;
 
groupsCopy = FOREACH groups GENERATE FLATTEN(resourceObjectsFlat.predicate) AS p1, FLATTEN(resourceObjectsFlat.predicate) AS p2;
propertyCooc = FILTER groupsCopy BY p1 < p2;
propertyCoocGroup = GROUP propertyCooc BY *;
propertyCoocCounts = FOREACH propertyCoocGroup GENERATE FLATTEN(group) AS (p1,p2), COUNT(propertyCooc) AS cnt;
propertyCoocSorted = ORDER propertyCoocCounts BY p1, cnt DESC;
propertyCoocSortedTop = FILTER propertyCoocSorted BY cnt > 1;