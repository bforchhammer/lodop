-- Property Co-Occurence By #Entities

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

subjGroups = GROUP quads BY subject;
 
subjGroupsCopy = FOREACH subjGroups GENERATE FLATTEN(quads.predicate) AS p1, FLATTEN(quads.predicate) AS p2;
propertyCooc = FILTER subjGroupsCopy BY p1 < p2;
propertyCoocGroup = GROUP propertyCooc BY *;
propertyCoocCounts = FOREACH propertyCoocGroup GENERATE FLATTEN(group) AS (p1,p2), COUNT(propertyCooc) AS cnt;
propertyCoocSorted = ORDER propertyCoocCounts BY p1, cnt DESC;
propertyCoocSortedTop = FILTER propertyCoocSorted BY cnt > 1;