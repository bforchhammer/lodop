-- Top Classes by Entity
REGISTER /usr/share/java/scala-library.jar;
REGISTER ldif-single-0.5.1-jar-with-dependencies.jar;
REGISTER loddesc-core-0.1.jar;

quads = LOAD 'file.nq'
	USING de.wbsg.loddesc.importer.QuadLoader()
	AS (subject:chararray, predicate:chararray, object:tuple(ntype:int,value:chararray,dtlang:chararray), graph:chararray);

typedstuff = FILTER quads BY predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
classes = GROUP typedstuff BY object.value;
classCounts = FOREACH classes GENERATE group, COUNT(typedstuff) AS cnt;
topClassesByEntities = ORDER classCounts BY cnt DESC;

STORE topClassesByEntities INTO 'results-topClassesByEntities';
